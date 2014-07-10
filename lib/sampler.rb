require_relative "helper"
require 'csv'

module DataSampler
  class Sampler
    attr_accessor :dataset, :db

    def initialize
      @dataset = {} # Convert this to a real class
      @db = Database.new(Configuration.db_configuration,
                         update_table_data = true,
                         fkey_tables=Configuration.fkey_tables)
    end


    def generate_data
      sample_tables_at_will
      constrained_sampling_of_tables
      write_dataset_to_files
    end


    def sample_tables_at_will
      # Go through each table and
      # Select random rows from them.

      db.tables.each do |t|
        sampled_data = t.sample_at_will
        update_data(t.name, sampled_data)
      end
    end

    def constrained_sampling_of_tables
      # Go through each set of data selected using 'sample_tables_at_will'
      # and select the appropriate 'ids' from the table that is referenced
      # as foreign key.
      db.tables.each do |t|
        t.columns.keys.each do |c|
          f_table = t.columns[c]

          next unless f_table
          sampled_data = t.sample_foreign_key_table(c, dataset)
          update_data(f_table.name, sampled_data)
        end
      end
    end

    def write_dataset_to_files
      sql_writer = SqlWriter.new("../data", dataset)
      sql_writer.write_sql_files
    end

    def update_data(table_name, sampled_data)
      if dataset[table_name]
        dataset[table_name] += Set.new(sampled_data)
      else
        dataset[table_name] = Set.new(sampled_data)
      end
    end
  end
end


DataSampler::Sampler.new.generate_data