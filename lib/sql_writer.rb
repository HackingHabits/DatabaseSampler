module DataSampler
  class SqlWriter
    def initialize(folder, dataset)
      @dataset = dataset
      @folder = folder
    end

    def write_sql_files
      @dataset.keys.each do |table_name|

        filename = @folder + "/" + table_name + ".csv"

        data = @dataset[table_name]

        if data && data.any?
          data = data.to_a
          columns = data[0].keys.sort
        else
          next
        end



        CSV.open(filename, 'w', { col_sep: ",", force_quotes: true }) do |writer|
          writer << columns
          data.each do |row|
            single_row = []
            columns.each do |column_name|

              val = row[column_name]
              if val.is_a?(Time)
                val = val.strftime("%Y-%m-%d %H:%M:%S")
              end

              if val.nil?
                single_row << '\N'
              else
                single_row << val
              end

            end
            writer << single_row
          end
        end

      end
    end
  end
end