package fonctions

import org.apache.spark.sql.{Column, DataFrame}
import constants._
import org.apache.spark.sql.functions._

object utils {


  def calculGlobal(dump_in: String, in_detail: String, voix_sms: String,
                   data: String, souscription: String, parc_orange: String,
                   daily_clients: String, master_data: String, debut: String, fin: String, date_parc: String,
                   subscribers: String, subscribers_full: String, ligne_prepaid: String, service_provider: String,
                   w_sms: String, debut_subs: String, fin_subs: String,
                   year_subs_full: String, month_subs_full: String, jour_lancement: String): DataFrame = {

    val df_telco = spark.sql(
      s"""
         |WITH
         |temp_dump_in AS (
         |    SELECT
         |        msisdn,
         |        brand AS formule,
         |        CAST(abs(compte) AS DOUBLE) AS solde_compteur,
         |        status
         |    FROM $dump_in
         |    WHERE day = '$fin' AND msisdn IS NOT NULL
         |),
         |
         |temp_in_detail AS (
         |    SELECT
         |        msisdn,
         |        SUM(montant) AS montant_recharge
         |    FROM $in_detail
         |    WHERE day BETWEEN '$debut' AND '$fin' AND msisdn IS NOT NULL
         |    GROUP BY msisdn
         |),
         |
         |temp_voix_sms AS (
         |    SELECT
         |        caller_msisdn AS msisdn,
         |        SUM(nombre) AS nombre_appel_sms
         |    FROM $voix_sms
         |    WHERE day BETWEEN '$debut' AND '$fin' AND caller_msisdn IS NOT NULL AND traffic_direction = 'SORTANT'
         |    GROUP BY caller_msisdn
         |),
         |
         |temp_data AS (
         |    SELECT
         |        msisdn,
         |        SUM(nombre) AS nombre_cons_data
         |    FROM $data
         |    WHERE day BETWEEN '$debut' AND '$fin' AND msisdn IS NOT NULL
         |    GROUP BY msisdn
         |),
         |
         |temp_recharge_bundles AS (
         |    SELECT
         |        msisdn,
         |        SUM(sous_mnt) AS montant_recharge_bundles
         |    FROM $souscription
         |    WHERE day BETWEEN '$debut' AND '$fin' AND msisdn IS NOT NULL
         |    AND types IN ('BIRD', 'BIRD/MIXEL', 'CIBLAGE', 'CHRONO', 'ILLIMIX', 'ILLIMIX_PRO', 'MIXEL')
         |    GROUP BY msisdn
         |),
         |
         |temp_recharge_illiflex AS (
         |    SELECT
         |        msisdn,
         |        SUM(sous_mnt) AS montant_recharge_illiflex
         |    FROM $souscription
         |    WHERE day BETWEEN '$debut' AND '$fin' AND msisdn IS NOT NULL
         |    AND types = 'ILLIFLEX'
         |    GROUP BY msisdn
         |),
         |
         |temp_recharge_pass_data AS (
         |    SELECT
         |        msisdn,
         |        SUM(sous_mnt) AS montant_recharge_pass_data
         |    FROM $souscription
         |    WHERE day BETWEEN '$debut' AND '$fin' AND msisdn IS NOT NULL
         |    AND types = 'PASS INTERNET'
         |    GROUP BY msisdn
         |),
         |
         |temp_parc_orange AS (
         |    SELECT
         |        msisdn,
         |        parc_actif
         |    FROM $parc_orange
         |    WHERE day = '$date_parc' AND msisdn IS NOT NULL
         |    GROUP BY msisdn, parc_actif
         |),
         |
         |temp_daily_clients AS (
         |    SELECT
         |        msisdn,
         |        date_premiere_activation,
         |        datediff('$jour_lancement', date_premiere_activation) AS anciennete_jour,
         |        floor(months_between('$jour_lancement', date_premiere_activation) / 12) AS anciennete_annee
         |    FROM $daily_clients
         |    WHERE day = '$fin' AND msisdn IS NOT NULL
         |    GROUP BY msisdn, date_premiere_activation
         |),
         |
         |temp_master_data AS (
         |    SELECT
         |        msisdn,
         |        imsi
         |    FROM $master_data
         |    WHERE day = '$fin' AND msisdn IS NOT NULL
         |),
         |
         |temp_ligne_prepaid AS (
         |    SELECT
         |        num_ligne     AS msisdn,
         |        d_resiliation AS resiliation
         |    FROM $ligne_prepaid
         |    WHERE day = '$fin' AND num_ligne IS NOT NULL
         |)
         |
         |SELECT
         |    a.msisdn,
         |    j.imsi,
         |    a.formule,
         |    COALESCE(b.montant_recharge, 0)           AS montant_recharge,
         |    COALESCE(c.nombre_appel_sms, 0)           AS nombre_appel_sms,
         |    COALESCE(d.nombre_cons_data, 0)           AS nombre_cons_data,
         |    COALESCE(e.montant_recharge_bundles, 0)   AS montant_recharge_bundles,
         |    COALESCE(f.montant_recharge_illiflex, 0)  AS montant_recharge_illiflex,
         |    COALESCE(g.montant_recharge_pass_data, 0) AS montant_recharge_pass_data,
         |    COALESCE(a.solde_compteur, 0)             AS solde_compteur,
         |    CASE WHEN (i.msisdn IS NOT NULL AND i.parc_actif = 1) THEN 'Oui' ELSE 'Non' END AS present_parc_orange,
         |    h.date_premiere_activation,
         |    h.anciennete_jour,
         |    h.anciennete_annee,
         |    a.status,
         |    k.resiliation,
         |    '$jour_lancement' AS jour_lancement
         |FROM
         |temp_dump_in                       a
         |LEFT  JOIN temp_in_detail          b          ON a.msisdn = b.msisdn
         |LEFT  JOIN temp_voix_sms           c          ON a.msisdn = c.msisdn
         |LEFT  JOIN temp_data               d          ON a.msisdn = d.msisdn
         |LEFT  JOIN temp_recharge_bundles   e          ON a.msisdn = e.msisdn
         |LEFT  JOIN temp_recharge_illiflex  f          ON a.msisdn = f.msisdn
         |LEFT  JOIN temp_recharge_pass_data g          ON a.msisdn = g.msisdn
         |LEFT  JOIN temp_daily_clients      h          ON a.msisdn = h.msisdn
         |LEFT  JOIN temp_parc_orange        i          ON a.msisdn = i.msisdn
         |LEFT  JOIN temp_master_data        j          ON a.msisdn = j.msisdn
         |LEFT  JOIN temp_ligne_prepaid      k          ON a.msisdn = k.msisdn
      """.stripMargin)

    val df_om = spark.sql(
      s"""
        |WITH
        |temp_subscribers AS (
        |   SELECT
        |       DISTINCT msisdn
        |   FROM $subscribers
        |   WHERE day BETWEEN '$debut_subs' AND '$fin_subs'
        |),
        |
        |temp_subscribers_full AS (
        |   SELECT
        |       DISTINCT msisdn
        |   FROM $subscribers_full
        |   WHERE year = '$year_subs_full' AND month = '$month_subs_full'
        |)
        |
        |SELECT
        |     msisdn
        |FROM temp_subscribers
        |UNION
        |SELECT
        |     msisdn
        |FROM temp_subscribers_full
      """.stripMargin).withColumnRenamed("msisdn", "msisdn_om")

    val df_wave = spark.sql(
      s"""
        |WITH service_provider AS (
        |    SELECT DISTINCT substring(msisdn, 4, 9) AS msisdn
        |    FROM $service_provider
        |    WHERE day BETWEEN '$debut' AND '$fin'
        |      AND portapp = '66780'
        |      AND LENGTH(msisdn) = 12
        |),
        |w_sms AS (
        |    SELECT DISTINCT b.msisdn
        |    FROM $w_sms a
        |    LEFT JOIN refined_trafic.master_data b
        |    ON a.imsi = b.imsi
        |    AND b.day BETWEEN '$debut' AND '$fin'
        |)
        |-- Selectionner les msisdn qui sont dans service_provider mais pas dans w_sms
        |SELECT msisdn
        |FROM service_provider
        |EXCEPT
        |SELECT msisdn
        |FROM w_sms
        |
        |UNION
        |
        |-- Selectionner les msisdn qui sont dans w_sms mais pas dans service_provider
        |SELECT msisdn
        |FROM w_sms
        |EXCEPT
        |SELECT msisdn
        |FROM service_provider
        |
        |UNION
        |
        |-- Selectionner les msisdn qui sont dans les deux tables
        |SELECT msisdn
        |FROM service_provider
        |INTERSECT
        |SELECT msisdn
        |FROM w_sms
      """.stripMargin).withColumnRenamed("msisdn", "msisdn_wave")

    val df_final = df_telco
      .join(df_om, df_telco("msisdn") === df_om("msisdn_om"), "left")
      .join(df_wave, df_telco("msisdn") === df_wave("msisdn_wave"), "left")
      .withColumn("client_om", when(df_om("msisdn_om").isNotNull, "Oui").otherwise("Non"))
      .withColumn("actif_wave", when(df_wave("msisdn_wave").isNotNull, "Oui").otherwise("Non"))
      .drop("msisdn_om", "msisdn_wave")

    df_final.select(
      "msisdn", "imsi", "formule", "montant_recharge", "nombre_appel_sms", "nombre_cons_data",
      "montant_recharge_bundles", "montant_recharge_illiflex", "montant_recharge_pass_data", "solde_compteur",
      "present_parc_orange", "date_premiere_activation", "anciennete_jour", "anciennete_annee", "status", "resiliation", "client_om",
      "actif_wave","jour_lancement"
    )

  }


  def resultatFinal(dataFrame: DataFrame): DataFrame = {

    dataFrame.select("msisdn", "imsi", "formule", "client_om", "actif_wave", "jour_lancement")
      .where((col("msisdn").startsWith("77") || col("msisdn").startsWith("78"))
        && col("formule").isin("Diamono CLASSIC", "Jamono Allo", "Kirene Avec Orange", "New KAO",
      "Orange Prepaid", "Jamono Max", "Jamono New S'cool", "Jamono Pro", "S'Cool Product")
        && col("montant_recharge") === 0 && col("nombre_appel_sms") === 0 &&
        col("nombre_cons_data") === 0 && col("montant_recharge_bundles") === 0 &&
        col("montant_recharge_illiflex") === 0 && col("montant_recharge_pass_data") === 0 &&
        col("solde_compteur") < 1000 && col("present_parc_orange") === "Non"
      && col("anciennete_jour") > 180 && col("anciennete_annee") < 10 && col("status") != 'G'
      && (col("resiliation") === "" || col("resiliation").isNull))
  }

}
