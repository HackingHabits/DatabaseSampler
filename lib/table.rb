module DataSampler
  class Table
    attr_accessor :name, :db, :columns, :ordered_columns, :foreign_key_tables

    # Manage various loop stopping conditions
    TOTAL_REQUIRED_ROWS = 2

    def initialize(db, name, update_foreign_tables = false)
      self.name = name
      self.db = db
      self.columns = {}
      self.foreign_key_tables = []
      update_columns

      if update_foreign_tables
        update_foreign_keys
      end
    end

    def update_columns
      begin
        result = db.main.query("show columns from #{name};")
      rescue Mysql2::Error
        return
      end

      result.each do |t|
        column_name = t["Field"]
        self.columns[column_name] = nil unless self.columns[column_name]
      end
    end

    def update_foreign_keys
      columns.keys.each do |c|
        reference_table = nil

        # Does it end with _id?
        if c.match(/_id$/)
          reference_table = c
        end

        if reference_table
          foreign_key_tables << Table.new(db, foreign_key_name_for(reference_table))
          columns[c] = Table.new(db, foreign_key_name_for(reference_table))
        end
      end
    end

    def get_max_rows
      max_rows = 0

      results = db.main.query("select table_rows from `information_schema`.tables where TABLE_NAME='#{self.name}' and TABLE_SCHEMA='#{Configuration.db_configuration[:database]}';")
      results.each do |t|
        max_rows = t["table_rows"]
        break
      end

      max_rows
    end

    def sample_using_offset
      # When id column does not exist
      # we can still sample data quickly
      # using a random offset
      sampled_rows = []

      max_rows = get_max_rows

      return sampled_rows if max_rows == 0

      total_rows = max_rows if max_rows < TOTAL_REQUIRED_ROWS

      random_offset = Random.rand(max_rows)

      results = db.main.query("select * from #{self.name} limit #{TOTAL_REQUIRED_ROWS} offset #{random_offset};")

      results.each do |row|
        sampled_rows << row
      end

      sampled_rows
    end


    def sample_at_will(key="id")
      # If 'id' column does not exist, sample using offset method
      return sample_using_offset if !self.columns.keys.include?(key)

      sampled_rows = []

      max_rows = get_max_rows

      return sampled_rows if max_rows == 0

      continue_sampling = true
      loop_counter = 0

      while continue_sampling
        random_ids = []
        (1..2).each do |i|
          random_ids << Random.rand(max_rows)
        end

        results = db.main.query("select * from #{self.name} where #{key} in (#{random_ids.join(',')});")
        results.each do |row|
            sampled_rows << row
        end

        loop_counter += 1
        continue_sampling = false if sampled_rows.length >= TOTAL_REQUIRED_ROWS
        continue_sampling = false if loop_counter > 1000
      end

      sampled_rows
    end


    def sample_foreign_key_table(column_name, dataset)
      fk_table = columns[column_name]

      fk_ids = []

      # The corresponding table does not exist
      # Nothing to sample
      return [] if dataset[self.name].nil? || dataset[fk_table.name].nil?

      dataset[self.name].each do |row|
        fk_ids << row[column_name] if row[column_name] && row[column_name] != "null"
      end

      fk_table.constrained_sampling(fk_ids)
    end

    def constrained_sampling(required_ids, key="id")
      sampled_rows = []

      return sampled_rows if !self.columns.keys.include?(key)
      return sampled_rows if required_ids.empty?

      # Filter out ids that are not numbers
      required_ids = required_ids.select { |i| i.to_s.match(/[0-9]+/) }
      required_ids.each_slice(5).to_a.each do |ids|
        results = db.main.query("select * from #{self.name} where #{key} in (#{ids.join(',')});")
        results.each do |row|
          sampled_rows << row
        end
      end

      sampled_rows
    end


    def foreign_key_name_for(t)
      # If we have been provided with a fkey_tables relationship
      # use that instead of guessing the fkey table name
      return self.db.fkey_tables[t] if self.db.fkey_tables[t]

      tb = t.slice(0,t.length-3)
      if tb.match(/y$/)
        "#{tb}ies"
      else
        "#{tb}s"
      end
    end

  end
end