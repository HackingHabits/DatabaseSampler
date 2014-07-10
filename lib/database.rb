module DataSampler
  class Database
    attr_accessor :main, :tables, :info, :fkey_tables

    def initialize(conn, update_table_data = false, fkey_tables={})
      information_schema_configuration = Configuration.db_configuration.dup
      information_schema_configuration[:database] = "INFORMATION_SCHEMA"

      @main = Mysql2::Client.new(Configuration.db_configuration)
      @info = Mysql2::Client.new(information_schema_configuration)
      @fkey_tables = fkey_tables
      @tables = []

      if update_table_data
        update_tables
      end
    end

    def update_tables
      # Search all tables in the db
      # and update tables
      results = main.query("show tables;")
      results.each do |t|
        table_name =  t["Tables_in_cdc_production"]
        tables << Table.new(self, table_name, true)
      end
    end

    def tables_without_foreign_keys
      tables.each do |t|
        if t.foreign_key_tables.empty?
          # No foreign key columns
          yield t
        end
      end
    end
  end

end
