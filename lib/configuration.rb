module DataSampler
  class Configuration
    DB_PORT = "3306"
    DB_HOST = "YOUDBHOST"
    DB_USER = "readonly"
    DB_PASSWORD = "YOUR PASSWORD"
    DB_NAME = "DATABASE NAME"

    def self.db_configuration
      {
          :host => DB_HOST,
          :username => DB_USER,
          :password => DB_PASSWORD,
          :port => DB_PORT,
          :database => DB_NAME
      }
    end


    def self.fkey_tables
       # When we cannot automatically descipher foreign_key_id as 'foreign_key' table
       # provide a mapping here. 
       # {foreign_key_id => foreign_key_table_name}
       {"table2_id" => "tables2"}
    end
  end
end

