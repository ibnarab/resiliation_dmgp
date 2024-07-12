package fonctions

import org.apache.spark.sql.SparkSession

object constants {

        val spark =

          SparkSession.builder

          .appName("multisim")

          .config("spark.hadoop.fs.defaultFS", "hdfs://bigdata")

          .config("spark.master", "yarn")

          .config("spark.submit.deployMode", "cluster")

          .enableHiveSupport()

          .getOrCreate()


        val base_dump_in                     =    "trusted_pfs.dumpin2"
        val base_in_detail                   =    "refined_recharge.recharge_in_detail"
        val base_voix_sms                    =    "refined_trafic.trafic_voix_sms"
        val base_data                        =    "refined_trafic.trafic_data"
        val base_souscription                =    "refined_vue360.daily_souscription"
        val base_parc_orange                 =    "refined_parc_orangesn.parc_orange"
        val base_daily_clients               =    "refined_vue360.daily_clients"
        val base_master_data                 =    "refined_trafic.master_data" //d_prem_active
        val base_subscribers                 =    "trusted_om.subscribers"
        val base_subscribers_full            =    "trusted_om.subscribers_full"
        val base_ligne_prepaid               =    "trusted_sicli.ligne_prepaid"
        val base_service_provider            =    "trusted_pfs.traffic_service_provider"
        val base_w_sms                       =    "analytics.w_sms"


        val resiliation_global               =    "dmgp_temp.resiliation_global"
        val resiliation_final                =    "dmgp_temp.resiliation"

}
