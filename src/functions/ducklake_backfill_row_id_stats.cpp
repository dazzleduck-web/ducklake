#include "functions/ducklake_table_functions.hpp"

#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_schema_entry.hpp"
#include "storage/ducklake_stats.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_transaction.hpp"

#include "duckdb/main/connection.hpp"

namespace duckdb {

//! Number of Parquet footers read per query
static constexpr idx_t BACKFILL_FOOTER_BATCH_SIZE = 1000;

struct BackfillTableResult {
	string schema_name;
	string table_name;
	TableIndex table_id;
	vector<DuckLakeRowIdStatsBackfillEntry> entries;
	idx_t embedded_count = 0;
	idx_t derived_count = 0;
	idx_t skipped_count = 0;
};

struct BackfillRowIdStatsBindData : public TableFunctionData {
	explicit BackfillRowIdStatsBindData(Catalog &catalog) : catalog(catalog) {
	}

	Catalog &catalog;
	vector<BackfillTableResult> tables;
	bool dry_run = false;
};

//! The row id column stats of a file, aggregated over its row groups
struct FooterRowIdStats {
	//! Whether the file has an embedded row id column
	bool has_row_id_column = false;
	idx_t row_group_count = 0;
	idx_t row_groups_with_bounds = 0;
	optional_idx min_row_id;
	optional_idx max_row_id;
	idx_t num_values = 0;
	idx_t row_groups_with_null_count = 0;
	idx_t null_count = 0;
	idx_t column_size_bytes = 0;
};

static string PathListLiteral(const vector<string> &paths) {
	string result = "[";
	for (auto &path : paths) {
		if (result.size() > 1) {
			result += ", ";
		}
		result += StringUtil::Format("%s", SQLString(path));
	}
	return result + "]";
}

//! Read the stats of the embedded row id column from the footers of the given files, with a single footer read per
//! file. Every file that was read is in the result, with or without a row id column. Runs on its own connection: a
//! missing or unreadable file fails the query, which must not abort the DuckLake transaction's metadata connection
static map<string, FooterRowIdStats> ReadFooterRowIdStats(Connection &footer_connection, const vector<string> &paths) {
	auto result = footer_connection.Query(StringUtil::Format(R"(
SELECT file_name,
       COUNT(*) FILTER (WHERE is_row_id),
       COUNT(*) FILTER (WHERE is_row_id AND min_row_id IS NOT NULL AND max_row_id IS NOT NULL),
       MIN(min_row_id) FILTER (WHERE is_row_id), MAX(max_row_id) FILTER (WHERE is_row_id),
       SUM(num_values) FILTER (WHERE is_row_id), COUNT(stats_null_count) FILTER (WHERE is_row_id),
       SUM(stats_null_count) FILTER (WHERE is_row_id), SUM(total_compressed_size) FILTER (WHERE is_row_id)
FROM (
    SELECT file_name, path_in_schema = '_ducklake_internal_row_id' AS is_row_id,
           TRY_CAST(COALESCE(stats_min_value, stats_min) AS BIGINT) AS min_row_id,
           TRY_CAST(COALESCE(stats_max_value, stats_max) AS BIGINT) AS max_row_id,
           stats_null_count, num_values, total_compressed_size
    FROM parquet_metadata(%s)
)
GROUP BY file_name
)",
	                                                         PathListLiteral(paths)));
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to read Parquet footers: ");
	}
	map<string, FooterRowIdStats> stats_per_file;
	for (auto &row : *result) {
		FooterRowIdStats stats;
		stats.row_group_count = row.GetValue<idx_t>(1);
		stats.has_row_id_column = stats.row_group_count > 0;
		stats.row_groups_with_bounds = row.GetValue<idx_t>(2);
		if (!row.IsNull(3) && !row.IsNull(4)) {
			stats.min_row_id = row.GetValue<idx_t>(3);
			stats.max_row_id = row.GetValue<idx_t>(4);
		}
		stats.num_values = row.IsNull(5) ? 0 : row.GetValue<idx_t>(5);
		stats.row_groups_with_null_count = row.GetValue<idx_t>(6);
		stats.null_count = row.IsNull(7) ? 0 : row.GetValue<idx_t>(7);
		stats.column_size_bytes = row.IsNull(8) ? 0 : row.GetValue<idx_t>(8);
		stats_per_file.emplace(row.GetValue<string>(0), stats);
	}
	return stats_per_file;
}

static DuckLakeRowIdStatsBackfillEntry EmbeddedRowIdEntry(DataFileIndex id, const FooterRowIdStats &stats) {
	DuckLakeRowIdStatsBackfillEntry entry;
	entry.id = id;
	// a range read from the embedded column has a column size - see DUCKLAKE_ROW_ID_FIELD_INDEX
	entry.column_size_bytes = MaxValue<idx_t>(stats.column_size_bytes, 1);
	if (stats.row_groups_with_null_count == stats.row_group_count && stats.null_count <= stats.num_values) {
		entry.value_count = stats.num_values - stats.null_count;
		entry.null_count = stats.null_count;
	}
	if (stats.row_groups_with_bounds == stats.row_group_count && stats.min_row_id.IsValid()) {
		entry.min_row_id = stats.min_row_id;
		entry.max_row_id = stats.max_row_id;
	}
	return entry;
}

static void BackfillTable(DuckLakeTransaction &transaction, Connection &footer_connection, DuckLakeTableEntry &table,
                          optional_idx &remaining_files, vector<BackfillTableResult> &results) {
	auto &metadata_manager = transaction.GetMetadataManager();
	auto files = metadata_manager.GetFilesWithoutRowIdStats(table);
	if (files.empty()) {
		return;
	}
	if (remaining_files.IsValid()) {
		if (remaining_files.GetIndex() == 0) {
			return;
		}
		if (files.size() > remaining_files.GetIndex()) {
			files.resize(remaining_files.GetIndex());
		}
		remaining_files = remaining_files.GetIndex() - files.size();
	}

	BackfillTableResult result;
	result.schema_name = table.ParentSchema().name;
	result.table_name = table.name;
	result.table_id = table.GetTableId();

	vector<reference<DuckLakeRowIdStatsBackfillFile>> readable_files;
	for (auto &file : files) {
		if (!file.data.encryption_key.empty()) {
			// reading an encrypted footer needs the file's key - not supported yet
			result.skipped_count++;
			continue;
		}
		readable_files.push_back(file);
	}

	for (idx_t batch_start = 0; batch_start < readable_files.size(); batch_start += BACKFILL_FOOTER_BATCH_SIZE) {
		auto batch_end = MinValue<idx_t>(batch_start + BACKFILL_FOOTER_BATCH_SIZE, readable_files.size());
		vector<string> paths;
		for (idx_t i = batch_start; i < batch_end; i++) {
			paths.push_back(readable_files[i].get().data.path);
		}

		map<string, FooterRowIdStats> footer_stats;
		unordered_set<string> unreadable_paths;
		try {
			footer_stats = ReadFooterRowIdStats(footer_connection, paths);
		} catch (std::exception &) {
			// a file in the batch is missing or unreadable - read the footers one by one to skip only those
			for (auto &path : paths) {
				try {
					auto file_stats = ReadFooterRowIdStats(footer_connection, {path});
					footer_stats.insert(file_stats.begin(), file_stats.end());
				} catch (std::exception &) {
					unreadable_paths.insert(path);
				}
			}
		}

		for (idx_t i = batch_start; i < batch_end; i++) {
			auto &file = readable_files[i].get();
			if (unreadable_paths.count(file.data.path)) {
				result.skipped_count++;
				continue;
			}
			auto entry = footer_stats.find(file.data.path);
			if (entry == footer_stats.end()) {
				// the footer of this file was not read (e.g. the path was expanded as a glob) - never guess its range
				result.skipped_count++;
				continue;
			}
			if (entry->second.has_row_id_column) {
				// the reader uses the embedded row ids
				result.entries.push_back(EmbeddedRowIdEntry(file.id, entry->second));
				result.embedded_count++;
				continue;
			}
			if (!file.row_id_start.IsValid() || file.row_count == 0) {
				result.skipped_count++;
				continue;
			}
			// no embedded row ids - the file's row ids are row_id_start + file_row_number
			DuckLakeRowIdStatsBackfillEntry derived;
			derived.id = file.id;
			derived.value_count = file.row_count;
			derived.null_count = 0;
			derived.min_row_id = file.row_id_start;
			derived.max_row_id = file.row_id_start.GetIndex() + file.row_count - 1;
			result.entries.push_back(derived);
			result.derived_count++;
		}
	}
	results.push_back(std::move(result));
}

static unique_ptr<FunctionData> DuckLakeBackfillRowIdStatsBind(ClientContext &context, TableFunctionBindInput &input,
                                                               vector<LogicalType> &return_types,
                                                               vector<string> &names) {
	auto &catalog = DuckLakeBaseMetadataFunction::GetCatalog(context, input.inputs[0]);
	auto &ducklake_catalog = catalog.Cast<DuckLakeCatalog>();
	auto result = make_uniq<BackfillRowIdStatsBindData>(catalog);

	string schema_name;
	string table_name;
	optional_idx remaining_files;
	for (auto &entry : input.named_parameters) {
		if (StringUtil::CIEquals(entry.first, "dry_run")) {
			result->dry_run = BooleanValue::Get(entry.second);
		} else if (StringUtil::CIEquals(entry.first, "schema")) {
			schema_name = StringValue::Get(entry.second);
		} else if (StringUtil::CIEquals(entry.first, "table_name")) {
			table_name = StringValue::Get(entry.second);
		} else if (StringUtil::CIEquals(entry.first, "max_files")) {
			if (entry.second.IsNull() || BigIntValue::Get(entry.second) <= 0) {
				throw BinderException("ducklake_backfill_row_id_stats: max_files must be greater than zero");
			}
			remaining_files = UBigIntValue::Get(entry.second.DefaultCastAs(LogicalType::UBIGINT));
		} else {
			throw InternalException("Unsupported named parameter for ducklake_backfill_row_id_stats");
		}
	}

	auto &transaction = DuckLakeTransaction::Get(context, catalog);
	Connection footer_connection(DatabaseInstance::GetDatabase(context));
	if (!table_name.empty()) {
		EntryLookupInfo table_lookup(CatalogType::TABLE_ENTRY, table_name, nullptr, QueryErrorContext());
		auto table_entry = catalog.GetEntry(context, schema_name, table_lookup, OnEntryNotFound::THROW_EXCEPTION);
		BackfillTable(transaction, footer_connection, table_entry->Cast<DuckLakeTableEntry>(), remaining_files,
		              result->tables);
	} else {
		for (auto &schema : ducklake_catalog.GetSchemas(context)) {
			if (!schema_name.empty() && !StringUtil::CIEquals(schema.get().name, schema_name)) {
				continue;
			}
			schema.get().Scan(context, CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
				if (entry.type == CatalogType::TABLE_ENTRY) {
					BackfillTable(transaction, footer_connection, entry.Cast<DuckLakeTableEntry>(), remaining_files,
					              result->tables);
				}
			});
		}
	}

	names.emplace_back("schema_name");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("table_name");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("files_with_embedded_row_ids");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("files_with_derived_row_ids");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("files_skipped");
	return_types.emplace_back(LogicalType::BIGINT);
	return std::move(result);
}

struct BackfillRowIdStatsState : public GlobalTableFunctionState {
	idx_t offset = 0;
	bool executed = false;
};

static unique_ptr<GlobalTableFunctionState> DuckLakeBackfillRowIdStatsInit(ClientContext &context,
                                                                           TableFunctionInitInput &input) {
	return make_uniq<BackfillRowIdStatsState>();
}

static void DuckLakeBackfillRowIdStatsExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->Cast<BackfillRowIdStatsBindData>();
	auto &state = data_p.global_state->Cast<BackfillRowIdStatsState>();
	if (!state.executed && !data.dry_run) {
		auto &transaction = DuckLakeTransaction::Get(context, data.catalog);
		auto &metadata_manager = transaction.GetMetadataManager();
		for (auto &table : data.tables) {
			metadata_manager.InsertRowIdStats(table.table_id, table.entries);
		}
	}
	state.executed = true;
	idx_t count = 0;
	while (state.offset < data.tables.size() && count < STANDARD_VECTOR_SIZE) {
		auto &table = data.tables[state.offset++];
		output.SetValue(0, count, Value(table.schema_name));
		output.SetValue(1, count, Value(table.table_name));
		output.SetValue(2, count, Value::BIGINT(NumericCast<int64_t>(table.embedded_count)));
		output.SetValue(3, count, Value::BIGINT(NumericCast<int64_t>(table.derived_count)));
		output.SetValue(4, count, Value::BIGINT(NumericCast<int64_t>(table.skipped_count)));
		count++;
	}
	output.SetCardinality(count);
}

DuckLakeBackfillRowIdStatsFunction::DuckLakeBackfillRowIdStatsFunction()
    : TableFunction("ducklake_backfill_row_id_stats", {LogicalType::VARCHAR}, DuckLakeBackfillRowIdStatsExecute,
                    DuckLakeBackfillRowIdStatsBind, DuckLakeBackfillRowIdStatsInit) {
	named_parameters["schema"] = LogicalType::VARCHAR;
	named_parameters["table_name"] = LogicalType::VARCHAR;
	named_parameters["max_files"] = LogicalType::BIGINT;
	named_parameters["dry_run"] = LogicalType::BOOLEAN;
}

} // namespace duckdb
