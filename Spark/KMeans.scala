from numpy import array

object KMeans{
    def main(args: Array[String]){
        println("Starting KMeans")
        dataFile = 'path to the file on yasser laptop'
        /* reading the data file and store it in an RDD through Spark context */ 
        data = sc.textFile(dataFile)
        /* parsing the data points and convert them to float */
        parsedData = data.map(line => array([float(x) for x in line.split(' ')]))
        /* caching the data for fast performance */
        parsedData.persist()
        parsedData.foreach(show)

        /* running the KMeans algorithm */


        /* after finishing using the data uncache it */
        parsedData.unpersist()
    }
}