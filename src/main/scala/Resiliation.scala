import fonctions.utils._
import fonctions.constants._
import fonctions.read_write._

object Resiliation {

  def main(args: Array[String]): Unit = {

      val debut           = args(0)
      val fin             = args(1)
      val date_orange     = args(2)
      val debut_subs      = args(3)
      val fin_subs        = args(4)
      val year_subs_full  = args(5)
      val month_subs_full = args(6)
      val jour_lancement  = args(7)

      val df = calculGlobal(base_dump_in, base_in_detail, base_voix_sms,
        base_data, base_souscription,
        base_parc_orange, base_daily_clients, base_master_data, debut, fin, date_orange,
        base_subscribers, base_subscribers_full, base_ligne_prepaid,
        base_service_provider, base_w_sms,debut_subs, fin_subs,
        year_subs_full, month_subs_full, jour_lancement)

      val df_final = resultatFinal(df)

      //writeHive(df, resiliation_global)
      //writeHive(df_final, resiliation_final)

    df.write
      .format("parquet")
      .mode("append")
      .option("compression", "gzip")
      .partitionBy("jour_lancement")
      .saveAsTable("dmgp_temp.resiliation_global")

    df_final.write
      .format("parquet")
      .mode("append")
      .option("compression", "gzip")
      .partitionBy("jour_lancement")
      .saveAsTable("dmgp_temp.resiliation")

  }

}
