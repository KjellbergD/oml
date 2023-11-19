#define DUCKDB_EXTENSION_MAIN

#include "oml_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include <fstream>
#include <iostream>
#include <utility>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

struct oml_append_information {
    duckdb::unique_ptr<InternalAppender> appender;
};

void AppendValues(ClientContext &context, TableCatalogEntry &tbl_catalog, vector<string> values) {
    auto append_info = make_uniq<oml_append_information>();
    append_info->appender = make_uniq<InternalAppender>(context, tbl_catalog);

    append_info->appender->BeginRow();
    append_info->appender->Append(Value(values[0]));
    append_info->appender->Append(Value(values[1]));
    append_info->appender->Append(Value(values[2]));
    append_info->appender->Append(Value(values[3]));
    append_info->appender->Append(Value(values[4]));
    append_info->appender->Append(Value::FLOAT(std::stof(values[5])));
    append_info->appender->Append(Value::FLOAT(std::stof(values[6])));
    append_info->appender->Append(Value::FLOAT(std::stof(values[7])));
    append_info->appender->EndRow();
}

struct DBGenFunctionData : public TableFunctionData {
	DBGenFunctionData() {
	}

	bool finished;
	string catalog;
	string schema;
    string filepath;
};

struct PowerConsumptionInfo {
	static const char *Name;
	static const idx_t ColumnCount;
	static const char *Columns[];
	static const LogicalType Types[];
    static const bool NotNullConstraints[];
};
const char *PowerConsumptionInfo::Name = "Power_Consumption";
const idx_t PowerConsumptionInfo::ColumnCount = 8;
const char *PowerConsumptionInfo::Columns[] = {"experiment_id", "node_id", "node_id_seq", "time_sec", "time_usec", "power", "current", "voltage"};
const LogicalType PowerConsumptionInfo::Types[] = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
                                        LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::FLOAT,
                                        LogicalType::FLOAT, LogicalType::FLOAT};
const bool PowerConsumptionInfo::NotNullConstraints[] = {false, false, false, true, true, true, true, true};

template <class T>
static void CreateTable(ClientContext &context, TableFunctionInput &data_p) {
    auto &data = (DBGenFunctionData &)*data_p.bind_data;

	auto info = make_uniq<CreateTableInfo>();
	info->catalog = data.catalog;
	info->schema = data.schema;
	info->table = T::Name;
	info->on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
	info->temporary = false;

	for (idx_t i = 0; i < T::ColumnCount; i++) {
		info->columns.AddColumn(ColumnDefinition(T::Columns[i], T::Types[i]));
        if (T::NotNullConstraints[i])
		    info->constraints.push_back(make_uniq<NotNullConstraint>(LogicalIndex(i)));
	}

	auto &catalog = Catalog::GetCatalog(context, data.catalog);
	catalog.CreateTable(context, std::move(info));
}

static void LoadOmlData(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &data = (DBGenFunctionData &)*data_p.bind_data;

    std::ifstream file(data.filepath);

    if (!file.is_open()) {
        throw InternalException("Could not open file");
    }

    std::string line;

    for (int i = 0; i < 9; i++) {
        if (!std::getline(file, line)) {
            throw InternalException("Expected metadata");
        }
    }

    auto &catalog = Catalog::GetCatalog(context, data.catalog);
    auto &tbl_catalog = catalog.GetEntry<TableCatalogEntry>(context, data.schema, "Power_Consumption");
    auto appender = make_uniq<InternalAppender>(context, tbl_catalog);

    idx_t row_count = 0;
    while (std::getline(file, line)) {
        std::istringstream iss(line);
        duckdb::vector<std::string> values;

        do {
            std::string value;
            iss >> value;
            if (!value.empty()) {
                values.push_back(value);
            }
        } while (iss);

        AppendValues(context, tbl_catalog, values);

        row_count++;
    }

    appender->Flush();
    appender.reset();

    output.SetValue(0, 0, Value::INTEGER(row_count));
    output.SetCardinality(1);

    data.finished = true;
    file.close();
}

static duckdb::unique_ptr<FunctionData> DbgenBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<DBGenFunctionData>();

    result->finished = false;
    result->catalog = "memory";
    result->schema = "main";
    result->filepath = StringValue::Get(input.inputs[0]);

	return_types.emplace_back(LogicalType::INTEGER);
	names.emplace_back("Rows inserted");

	return std::move(result);
}

static void DbgenFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = (DBGenFunctionData &)*data_p.bind_data;
    
	if (data.finished) {
		return;
	}

	CreateTable<PowerConsumptionInfo>(context, data_p);
	LoadOmlData(context, data_p, output);

	data.finished = true;
}

static void LoadInternal(DatabaseInstance &instance) {
    TableFunction power_consumption_gen("Power_Consumption_load", {LogicalType::VARCHAR}, DbgenFunction, DbgenBind);
	ExtensionUtil::RegisterFunction(instance, power_consumption_gen);
}

void OmlExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string OmlExtension::Name() {
	return "oml";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void oml_init(duckdb::DatabaseInstance &db) {
	LoadInternal(db);
}

DUCKDB_EXTENSION_API const char *oml_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
