#include "datalake/serde_parquet_writer.h"

#include "base/vlog.h"
#include "bytes/bytes.h"
#include "bytes/iobuf_parser.h"
#include "datalake/logger.h"
#include "iceberg/conversion/schema_parquet.h"
#include "iceberg/conversion/values_parquet.h"
#include "serde/parquet/metadata.h"
#include "version/version.h"

#include <seastar/util/defer.hh>

#include <bit>
#include <cmath>
#include <variant>

namespace datalake {
namespace {

/// Returns true if the raw bound value encodes a floating-point NaN.
/// Used only in debug assertions; non-float types always return false.
[[maybe_unused]] bool
bound_is_nan(const iobuf& raw, const serde::parquet::physical_type& phys) {
    return std::visit(
      [&]<typename T>(const T&) -> bool {
          if constexpr (std::is_same_v<T, serde::parquet::f32_type>) {
              iobuf_parser p(raw.copy());
              return std::isnan(
                std::bit_cast<float>(
                  ss::le_to_cpu(p.consume_type<uint32_t>())));
          } else if constexpr (std::is_same_v<T, serde::parquet::f64_type>) {
              iobuf_parser p(raw.copy());
              return std::isnan(
                std::bit_cast<double>(
                  ss::le_to_cpu(p.consume_type<uint64_t>())));
          } else {
              return false;
          }
      },
      phys);
}

/// Returns true if raw_a < raw_b for the given parquet physical type.
/// Values are in PLAIN encoding (same as Iceberg binary single-value format).
bool bound_less_than(
  const iobuf& raw_a,
  const iobuf& raw_b,
  const serde::parquet::physical_type& phys) {
    return std::visit(
      [&]<typename T>(const T&) -> bool {
          if constexpr (std::is_same_v<T, serde::parquet::i32_type>) {
              iobuf_parser pa(raw_a.copy()), pb(raw_b.copy());
              return ss::le_to_cpu(pa.consume_type<int32_t>())
                     < ss::le_to_cpu(pb.consume_type<int32_t>());
          } else if constexpr (std::is_same_v<T, serde::parquet::i64_type>) {
              iobuf_parser pa(raw_a.copy()), pb(raw_b.copy());
              return ss::le_to_cpu(pa.consume_type<int64_t>())
                     < ss::le_to_cpu(pb.consume_type<int64_t>());
          } else if constexpr (std::is_same_v<T, serde::parquet::f32_type>) {
              iobuf_parser pa(raw_a.copy()), pb(raw_b.copy());
              float a = std::bit_cast<float>(
                ss::le_to_cpu(pa.consume_type<uint32_t>()));
              float b = std::bit_cast<float>(
                ss::le_to_cpu(pb.consume_type<uint32_t>()));
              return a < b;
          } else if constexpr (std::is_same_v<T, serde::parquet::f64_type>) {
              iobuf_parser pa(raw_a.copy()), pb(raw_b.copy());
              double a = std::bit_cast<double>(
                ss::le_to_cpu(pa.consume_type<uint64_t>()));
              double b = std::bit_cast<double>(
                ss::le_to_cpu(pb.consume_type<uint64_t>()));
              return a < b;
          } else if constexpr (
            std::is_same_v<T, serde::parquet::byte_array_type>) {
              return iobuf_to_bytes(raw_a.copy())
                     < iobuf_to_bytes(raw_b.copy());
          } else if constexpr (std::is_same_v<T, serde::parquet::bool_type>) {
              iobuf_parser pa(raw_a.copy()), pb(raw_b.copy());
              return pa.consume_type<uint8_t>() < pb.consume_type<uint8_t>();
          } else {
              return false;
          }
      },
      phys);
}

chunked_vector<per_column_stats>
extract_column_stats(const serde::parquet::file_metadata& meta) {
    // Collect leaf columns in DFS order: they correspond 1:1 with the
    // column_chunks in each row group.
    struct leaf_info {
        std::optional<int32_t> field_id;
        serde::parquet::physical_type phys;
    };
    chunked_vector<leaf_info> leaves;
    for (const auto& s : meta.schema) {
        if (
          s.num_children == 0
          && !std::holds_alternative<std::monostate>(s.type)) {
            leaves.push_back({s.field_id, s.type});
        }
    }

    chunked_vector<per_column_stats> result;
    result.reserve(leaves.size());

    for (size_t j = 0; j < leaves.size(); ++j) {
        const auto& leaf = leaves[j];
        if (!leaf.field_id) {
            continue;
        }

        per_column_stats cs;
        cs.field_id = *leaf.field_id;

        std::optional<iobuf> agg_min, agg_max;

        for (const auto& rg : meta.row_groups) {
            if (j >= rg.columns.size()) {
                continue;
            }
            const auto& cmd = rg.columns[j].meta_data;
            cs.value_count += cmd.num_values;
            cs.column_size_bytes += cmd.total_compressed_size;

            if (!cmd.stats) {
                continue;
            }
            const auto& stats = *cmd.stats;

            if (stats.null_count) {
                cs.null_value_count += *stats.null_count;
            }
            if (stats.min) {
                dassert(
                  !bound_is_nan(stats.min->value, leaf.phys),
                  "Parquet min bound must not be NaN; column_stats_collector "
                  "should have filtered it");
                if (
                  !agg_min
                  || bound_less_than(stats.min->value, *agg_min, leaf.phys)) {
                    agg_min = stats.min->value.copy();
                }
            }
            if (stats.max) {
                dassert(
                  !bound_is_nan(stats.max->value, leaf.phys),
                  "Parquet max bound must not be NaN; column_stats_collector "
                  "should have filtered it");
                if (
                  !agg_max
                  || bound_less_than(*agg_max, stats.max->value, leaf.phys)) {
                    agg_max = stats.max->value.copy();
                }
            }
        }

        if (agg_min) {
            cs.lower_bound = iobuf_to_bytes(*agg_min);
        }
        if (agg_max) {
            cs.upper_bound = iobuf_to_bytes(*agg_max);
        }

        result.push_back(std::move(cs));
    }

    return result;
}

} // namespace

writer_error serde_parquet_writer::set_error(writer_error e) {
    _error = e;
    return _error;
}

ss::future<writer_error> serde_parquet_writer::add_data_struct(
  iceberg::struct_value value, size_t, ss::abort_source& as) {
    // This method should always return `writer_error::ok` if
    // `_writer.write_row(...)` is successful. However, we still want to convey
    // memory and disk reservation errors that happen after the row write.
    // Hence, the current solution is to return those errors on the subsequent
    // call to `add_data_struct`.
    //
    // Similar to `local_parquet_file_writer` once an error has occurred further
    // writes are prevented.
    if (_error != writer_error::ok) {
        co_return _error;
    }

    auto conversion_result = co_await to_parquet_value(
      std::make_unique<iceberg::struct_value>(std::move(value)));
    if (conversion_result.has_error()) {
        vlog(
          datalake_log.warn,
          "Error converting iceberg struct to parquet value - {}",
          conversion_result.error());
        co_return set_error(writer_error::parquet_conversion_error);
    }

    auto group = std::get<serde::parquet::group_value>(
      std::move(conversion_result.value()));
    try {
        auto stats = co_await _writer.write_row(std::move(group));
        auto stats_updater = ss::defer([this, stats] {
            _buffered_bytes = stats.buffered_size;
            _flushed_bytes = stats.flushed_size;
        });

        /*
         * handle disk reservation. see writer_disk_tracker for more info.
         */
        const auto total_bytes = _buffered_bytes + _flushed_bytes;
        const auto new_total_bytes = stats.buffered_size + stats.flushed_size;
        if (new_total_bytes > total_bytes) {
            auto& disk = _mem_tracker.disk();
            auto result = co_await disk.reserve_bytes(
              new_total_bytes - total_bytes, as);
            if (result != reservation_error::ok) {
                set_error(map_to_writer_error(result));
                co_return writer_error::ok;
            }
        } else if (new_total_bytes < total_bytes) {
            auto& disk = _mem_tracker.disk();
            co_await disk.free_bytes(total_bytes - new_total_bytes, as);
        }

        /*
         * handle memory reservation
         */
        auto new_buffered_bytes = stats.buffered_size;
        if (new_buffered_bytes > _buffered_bytes) {
            auto reservation_result = co_await _mem_tracker.reserve_bytes(
              new_buffered_bytes - _buffered_bytes, as);
            if (reservation_result != reservation_error::ok) {
                set_error(map_to_writer_error(reservation_result));
                co_return writer_error::ok;
            }
        } else if (new_buffered_bytes < _buffered_bytes) {
            // underlying writer may choose to compress data when
            // a page worth of data is batched, at which point the
            // resulting compressed size is smaller than before and
            // allows us to free up some bytes.
            co_await _mem_tracker.free_bytes(
              _buffered_bytes - new_buffered_bytes, as);
        }
    } catch (...) {
        vlog(
          datalake_log.warn,
          "Error writing parquet row - {}",
          std::current_exception());
        co_return set_error(writer_error::file_io_error);
    }
    co_return writer_error::ok;
}

size_t serde_parquet_writer::buffered_bytes() const { return _buffered_bytes; }
size_t serde_parquet_writer::flushed_bytes() const { return _flushed_bytes; }

ss::future<> serde_parquet_writer::flush() {
    co_await _writer.flush_row_group();
    auto stats = _writer.stats();
    _buffered_bytes = stats.buffered_size;
    _flushed_bytes = stats.flushed_size;
    vassert(
      _buffered_bytes == 0,
      "Memory buffered in the writer after flush: {}",
      _buffered_bytes);
}

ss::future<writer_error> serde_parquet_writer::finish() {
    auto meta = co_await _writer.close();
    _column_stats = extract_column_stats(meta);
    _buffered_bytes = _flushed_bytes = 0;
    co_return writer_error::ok;
}

chunked_vector<per_column_stats> serde_parquet_writer::column_stats() const {
    return _column_stats.copy();
}

ss::future<std::unique_ptr<parquet_ostream>>
serde_parquet_writer_factory::create_writer(
  const iceberg::struct_type& schema,
  ss::output_stream<char> out,
  writer_mem_tracker& mem_tracker) {
    serde::parquet::writer::options opts{
      .schema = schema_to_parquet(schema),
      .version = ss::sstring(redpanda_git_version()),
      .build = ss::sstring(redpanda_git_revision()),
      .compress = true,
    };
    serde::parquet::writer writer(std::move(opts), std::move(out));
    co_await writer.init();
    co_return std::make_unique<serde_parquet_writer>(
      std::move(writer), mem_tracker);
}

} // namespace datalake
