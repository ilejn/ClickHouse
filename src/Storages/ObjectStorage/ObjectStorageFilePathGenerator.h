#pragma once

#include <string>
#include <Storages/PartitionedSink.h>
#include <Poco/String.h>
#include <Functions/generateSnowflakeID.h>

namespace DB
{
    struct ObjectStorageFilePathGenerator
    {
        virtual ~ObjectStorageFilePathGenerator() = default;
        virtual std::string getWritingPath(const std::string & partition_id, const std::string & /* file_name_override */) const = 0;
        virtual std::string getReadingPath() const = 0;
    };

    struct ObjectStorageWildcardFilePathGenerator : ObjectStorageFilePathGenerator
    {
        explicit ObjectStorageWildcardFilePathGenerator(const std::string & raw_path_) : raw_path(raw_path_) {}

        std::string getWritingPath(const std::string & partition_id, const std::string & /* file_name_override */) const override
        {
            return PartitionedSink::replaceWildcards(raw_path, partition_id);
        }

        std::string getReadingPath() const override
        {
            return raw_path;
        }

    private:
        std::string raw_path;

    };

    struct ObjectStorageAppendFilePathGenerator : ObjectStorageFilePathGenerator
    {
        explicit ObjectStorageAppendFilePathGenerator(
            const std::string & raw_path_,
            const std::string & file_format_)
        : raw_path(raw_path_), file_format(Poco::toLower(file_format_)){}

        std::string getWritingPath(const std::string & partition_id, const std::string & file_name_override) const override
        {
            const auto file_name = file_name_override.empty() ? std::to_string(generateSnowflakeID()) : file_name_override;
            return raw_path + "/" + partition_id + "/"  + file_name + "." + file_format;
        }

        std::string getReadingPath() const override
        {
            return raw_path + "**." + file_format;
        }

    private:
        std::string raw_path;
        std::string file_format;
    };

}
