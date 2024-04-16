# multiple-sinks-with-sparkstreaming
This project aims to read the streamed data related to three different sensors and write them to three different sinks (hadoop, local postgresql database and delta table on hdfs) depending on their device id . 

@erkansirin76's data-generator tool is used to simulate a data stream by constantly streaming the static data.
