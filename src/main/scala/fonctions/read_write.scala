package fonctions

import org.apache.spark.sql.{DataFrame, SaveMode}


object read_write {

        def writeHive(dataFrame: DataFrame, table: String) : Unit =  {

          dataFrame

              .repartition(1)

              .write
            

              .mode(SaveMode.Append)

              .partitionBy("jour_lancement")

              .option("header", true)

              .option("compression", "gzip")

              //.option("path", path)

              .saveAsTable(table)

        }

}
