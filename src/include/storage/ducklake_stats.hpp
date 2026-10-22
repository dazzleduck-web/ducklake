//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_stats.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/ducklake_extra_stats.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"

namespace duckdb {
class BaseStatistics;
struct DuckLakeDataFile;

//! The per-file row id range is stored as column stats under the reserved row id field id, the field id that
//! `_ducklake_internal_row_id` is written with. A range read from that embedded column has a column_size_bytes > 0,
//! a range derived from row_id_start has column_size_bytes = 0
static constexpr idx_t DUCKLAKE_ROW_ID_FIELD_INDEX = MultiFileReader::ROW_ID_FIELD_ID;

inline bool IsRowIdStatsField(FieldIndex field_index) {
	return field_index.index == DUCKLAKE_ROW_ID_FIELD_INDEX;
}

//! Returns true for types that require value-based (not lexicographic string) comparison for min/max stats
inline bool RequiresValueComparison(const LogicalType &type) {
	return type.IsNumeric() || type.IsTemporal() || type.id() == LogicalTypeId::BOOLEAN;
}

struct DuckLakeColumnStats;
struct DuckLakeGlobalColumnStatsInfo;

struct DuckLakeColumnStats {
	explicit DuckLakeColumnStats(LogicalType type_p);

	// Copy constructor
	DuckLakeColumnStats(const DuckLakeColumnStats &other);
	DuckLakeColumnStats &operator=(const DuckLakeColumnStats &other);
	DuckLakeColumnStats(DuckLakeColumnStats &&other) noexcept = default;
	DuckLakeColumnStats &operator=(DuckLakeColumnStats &&other) noexcept = default;

	LogicalType type;
	string min;
	string max;
	idx_t null_count = 0;
	idx_t num_values = 0;
	idx_t column_size_bytes = 0;
	bool contains_nan = false;
	bool has_null_count = false;
	bool has_num_values = false;
	bool has_min = false;
	bool has_max = false;
	bool any_valid = true;
	//! Invalidated bounds must never be reseeded
	bool bounds_unknown = false;
	bool has_contains_nan = false;

	bool AnyValid() const {
		if (has_num_values && has_null_count) {
			return num_values > null_count;
		}
		return any_valid;
	}

	unique_ptr<DuckLakeColumnExtraStats> extra_stats;

public:
	static DuckLakeColumnStats FromGlobalStats(const LogicalType &type, const DuckLakeGlobalColumnStatsInfo &col,
	                                           bool table_has_rows);
	static bool BoundsSurviveTypePromotion(const LogicalType &source, const LogicalType &target);
	//! Row id stats of a file without an embedded row id column: [row_id_start, row_id_start + row_count - 1]
	static DuckLakeColumnStats RowIdRangeStats(idx_t row_id_start, idx_t row_count);
	unique_ptr<BaseStatistics> ToStats() const;
	void MergeStats(const DuckLakeColumnStats &new_stats);

private:
	unique_ptr<BaseStatistics> CreateNumericStats() const;
	unique_ptr<BaseStatistics> CreateStringStats() const;
	unique_ptr<BaseStatistics> CreateVariantStats() const;
	unique_ptr<BaseStatistics> CreateGeometryStats() const;
};

//! These are the global, table-wide stats
struct DuckLakeTableStats {
	idx_t record_count = 0;
	idx_t table_size_bytes = 0;
	idx_t next_row_id = 0;
	map<FieldIndex, DuckLakeColumnStats> column_stats;

	void MergeStats(FieldIndex col_id, const DuckLakeColumnStats &file_stats);

	void MergeFileStats(const DuckLakeDataFile &file);
};

struct DuckLakeStats {
	map<TableIndex, unique_ptr<DuckLakeTableStats>> table_stats;
};

} // namespace duckdb
