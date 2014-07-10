Database Sampler
=========

Samples a subset of your production database and provides a small sized dataset that represents true data. All associations are maintained to provide 
the best representation of the dataset. 

  ruby sampler.rb

This will generate csv files in the `data` folder. You should be able to import
the data into mysql using the load script provided

 bash ./load_data.sh

Future Roadmap
===========
