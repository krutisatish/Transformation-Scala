package com.kogentix.starcom.util

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.log4j._
import org.apache.spark.sql.functions.round

trait LogHelper {
  val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)
}

object transformation extends LogHelper {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("SparkTransformation")
      .master("yarn")
      .config("spark.sql.shuffle.partitions",2048)
      .config("spark.yarn.executor.memoryOverhead","4G")
      .enableHiveSupport()
      .getOrCreate()

    import org.apache.spark.sql.types._

    logger.info("Getting the current date from the shell script")
    val current_date = args(0)


    logger.info("Match table Schema Definition and creating dataframes for advertisers table and broadcasting it")

    val schema_advertisers :StructType = new StructType()
      .add("floodlight_configuration", LongType)
      .add("advertiser_id", LongType)
      .add("advertiser", StringType)
      .add("advertiser_group_id", LongType)
      .add("advertiser_group", StringType)
    val advertisers_tmp= spark.read.option("header", true).schema(schema_advertisers).csv("/data/hive/rawdb/advertisers/client=kellogg/file_date="+current_date)
    val advertisers_tmp1 = advertisers_tmp.drop("floodlight_configuration").drop("advertiser_group_id").drop("advertiser_group")
    val advertisers_tmp2 =advertisers_tmp1.withColumnRenamed("advertiser_id","avd")
    val advertisers = broadcast(advertisers_tmp2)

    logger.info("Match table Schema Definition and creating dataframes for campaigns table and broadcasting it")

    val schema_campaigns :StructType = new StructType()
      .add("advertiser_id", LongType).add("campaign_id", LongType)
      .add("campaign", StringType)
      .add("campaign_start_date", StringType)
      .add("campaign_end_date", StringType)
      .add("billing_invoice_code", StringType)
    val campaigns_tmp = spark.read.option("header", true).schema(schema_campaigns).csv("/data/hive/rawdb/campaigns/client=kellogg/file_date="+current_date)
    val campaigns_tmp1 = campaigns_tmp.drop("advertiser_id").drop("campaign_start_date").drop("campaign_end_date").drop("billing_invoice_code")
    val campaigns_tmp2 = campaigns_tmp1.withColumnRenamed("campaign_id","cam")
    val campaigns = broadcast(campaigns_tmp2)

    logger.info("Match table Schema Definition and creating dataframes for ads table and broadcasting it")

    val schema_ads :StructType = new StructType()
      .add("advertiser_id", LongType)
      .add("campaign_id", LongType)
      .add("ad_id", LongType)
      .add("ad", StringType)
      .add("ad_click_url", StringType)
      .add("ad_type", StringType)
      .add("creative_pixel_size", StringType)
      .add("ad_comments", StringType)
    val ads_tmp = spark.read.option("header", true).schema(schema_ads).csv("/data/hive/rawdb/ads/client=kellogg/file_date="+current_date)
    val ads_tmp1 = ads_tmp.drop("advertiser_id").drop("campaign_id").drop("ad_click_url").drop("ad_type").drop("creative_pixel_size").drop("ad_comments")
    val ads_tmp2 = ads_tmp1.withColumnRenamed("ad_id","adi")
    val ads = broadcast(ads_tmp2)

    logger.info("Match table Schema Definition and creating dataframes for creatives table and broadcasting it")

    val schema_creatives:StructType = new StructType()
      .add("advertiser_id", LongType)
      .add("rendering_id", LongType)
      .add("creative_id", LongType)
      .add("creative", StringType)
      .add("creative_last_modified_date", LongType)
      .add("creative_type", StringType)
      .add("creative_pixel_size", StringType)
      .add("creative_image_url", StringType)
      .add("creative_version", IntegerType)
    val creatives_tmp = spark.read.option("header", true).schema(schema_creatives).csv("/data/hive/rawdb/creatives/client=kellogg/file_date="+current_date)
    val creatives_tmp1 = creatives_tmp.drop("advertiser_id").drop("creative_last_modified_date").drop("creative_type").drop("creative_pixel_size").drop("creative_image_url").drop("creative_version")
    val creatives_tmp2 = creatives_tmp1.withColumnRenamed("rendering_id","ren")
    val creatives = broadcast(creatives_tmp2)

    logger.info("Match table Schema Definition and creating dataframes for sites table and broadcasting it")

    val schema_sites :StructType = new StructType()
      .add("site_id_dcm", LongType)
      .add("site_dcm", StringType)
      .add("site_id_site_directory", LongType)
      .add("site_site_directory", StringType)
    val sites_tmp = spark.read.option("header", true).schema(schema_sites).csv("/data/hive/rawdb/sites/client=kellogg/file_date="+current_date)
    val sites_tmp1 = sites_tmp.drop("site_id_sitedirectory").drop("site_sitedirectory")
    val sites_tmp2 = sites_tmp1.withColumnRenamed("site_id_dcm","sit")
    val sites =broadcast(sites_tmp2)

    logger.info("Match table Schema Definition and creating dataframes for placements table and broadcasting it")

    val schema_placements :StructType = new StructType()
      .add("campaign_id", LongType)
      .add("site_id_dcm", LongType)
      .add("placement_id", LongType)
      .add("site_keyname", StringType)
      .add("placement", StringType)
      .add("content_category", StringType)
      .add("placement_strategy", StringType)
      .add("placement_start_date", StringType)
      .add("placement_end_date", StringType)
      .add("placement_group_type", StringType)
      .add("packageroad_block_id", LongType)
      .add("placement_cost_structure", StringType)
      .add("placement_cap_cost_option", StringType)
      .add("activity_id", LongType)
      .add("flighting_activated", BooleanType)
    val placements_tmp = spark.read.option("header", true).schema(schema_placements).csv("/data/hive/rawdb/placements/client=kellogg/file_date="+current_date)
    val placements_tmp1 = placements_tmp.drop("campaign_id").drop("site_id_dcm").drop("site_keyname").drop("content_category").drop("placement_strategy").drop("placement_start_date").drop("placement_end_date").drop("placement_group_type").drop("packageroad_block_id").drop("placement_cost_structure").drop("placement_cap_cost_option").drop("activity_id").drop("flighting_activated")
    val placements_tmp2 = placements_tmp1.withColumnRenamed("placement_id","pla")
    val placements = broadcast(placements_tmp2)

    logger.info("Match table Schema Definition and creating dataframes for states_mod table and broadcasting it")

    val schema_states :StructType = new StructType()
      .add("state_region", StringType)
      .add("state_region_full_name", StringType)
    val states = spark.read.option("header", true).schema(schema_states).csv("/data/hive/rawdb/states/client=kellogg/file_date="+current_date)
    val states_mod_tmp = states.select(substring(col("state_region"),4,2).as("region_matchedcol"),col("state_region_full_name"),substring(col("state_region"),1,2).as("region"))
    val states_mod = broadcast(states_mod_tmp)

    logger.info("Match table Schema Definition and creating dataframes for browsers table and broadcasting it")

    val schema_browsers :StructType = new StructType()
      .add("browser_platform_id", LongType)
      .add("browser_platform", StringType)
    val browsers_tmp = spark.read.option("header", true).schema(schema_browsers).csv("/data/hive/rawdb/browsers/client=kellogg/file_date="+current_date)
    val browsers_tmp2 = browsers_tmp.withColumnRenamed("browser_platform_id","bro")
    val browsers = broadcast(browsers_tmp2)

    logger.info("Match table Schema Definition and creating dataframes for operating_systems table and broadcasting it")

    val schema_operating_systems :StructType = new StructType()
      .add("operating_system_id", LongType)
      .add("operating_system", StringType)
    val operating_systems_tmp = spark.read.option("header", true).schema(schema_operating_systems).csv("/data/hive/rawdb/operating_systems/client=kellogg/file_date="+current_date)
    val operating_systems_tmp1 = operating_systems_tmp.withColumnRenamed("operating_system_id","ope")
    val operating_systems = broadcast(operating_systems_tmp1)

    logger.info("Match table Schema Definition and creating dataframes for designated_market_areas table and broadcasting it")

    val schema_designated_market_areas :StructType = new StructType()
      .add("designated_market_area_dma_id",LongType)
      .add("designated_market_area_dma", StringType)
    val designated_market_areas_tmp = spark.read.option("header", true).schema(schema_designated_market_areas).csv("/data/hive/rawdb/designated_market_areas/client=kellogg/file_date="+current_date)
    val designated_market_areas_tmp1 = designated_market_areas_tmp.withColumnRenamed("designated_market_area_dma_id", "des")
    val designated_market_areas = broadcast(designated_market_areas_tmp1)

    logger.info("Match table Schema Definition and creating dataframes for cities table and broadcasting it")

    val schema_cities : StructType = new StructType()
      .add("city_id", LongType)
      .add("city", StringType)
    val cities_tmp = spark.read.option("header", true).schema(schema_cities).csv("/data/hive/rawdb/cities/client=kellogg/file_date="+current_date)
    val cities_tmp1 = cities_tmp.withColumnRenamed("city_id","cit")
    val cities = broadcast(cities_tmp1)

    logger.info("Impression table Schema Definition and creating dataframe")

    val schema_impression : StructType =new StructType()
      .add("event_time", LongType)
      .add("user_id", StringType)
      .add("advertiser_id", LongType)
      .add("campaign_id", LongType)
      .add("ad_id", LongType)
      .add("rendering_id", LongType)
      .add("creative_version", IntegerType)
      .add("site_id_dcm", LongType)
      .add("placement_id", LongType)
      .add("country_code", StringType)
      .add("state_region", StringType)
      .add("browser_platform_id", LongType)
      .add("browser_platform_version", StringType)
      .add("operating_system_id", LongType)
      .add("designated_market_area_dma_id", LongType)
      .add("city_id", LongType)
      .add("zip_postal_code", StringType)
      .add("u_value", StringType)
      .add("event_type", StringType)
      .add("event_subtype", StringType)
      .add("dbm_auction_id", StringType)
      .add("dbm_request_time", LongType)
      .add("dbm_advertiser_id", LongType)
      .add("dbm_insertion_order_id", LongType)
      .add("dbm_line_item_id", LongType)
      .add("dbm_creative_id", LongType)
      .add("dbm_bid_price_usd", LongType)
      .add("dbm_bid_price_partner_currency", LongType)
      .add("dbm_bid_price_advertiser_currency", LongType)
      .add("dbm_url", StringType)
      .add("dbm_site_id", LongType)
      .add("dbm_language", StringType)
      .add("dbm_adx_page_categories", StringType)
      .add("dbm_matching_targeted_keywords", StringType)
      .add("dbm_exchange_id", LongType)
      .add("dbm_attributed_inventory_source_external_id", StringType)
      .add("dbm_attributed_inventory_source_is_public", BooleanType)
      .add("dbm_ad_position", LongType)
      .add("dbm_country_code", StringType)
      .add("dbm_designated_market_area_dma_id", LongType)
      .add("dbm_zip_postal_code", StringType)
      .add("dbm_state_region_id", LongType)
      .add("dbm_city_id", LongType)
      .add("dbm_operating_system_id", LongType)
      .add("dbm_browser_platform_id", LongType)
      .add("dbm_browser_timezone_offset_minutes", LongType)
      .add("dbm_net_speed", LongType)
      .add("dbm_matching_targeted_segments", StringType)
      .add("dbm_isp_id", LongType)
      .add("dbm_device_type", LongType)
      .add("dbm_mobile_make_id", LongType)
      .add("dbm_mobile_model_id", LongType)
      .add("dbm_view_state", StringType)
      .add("partner1_id", StringType)
      .add("partner2_id", StringType)
      .add("dbm_media_cost_usd", LongType)
      .add("dbm_media_cost_partner_currency", LongType)
      .add("dbm_media_cost_advertiser_currency", LongType)
      .add("dbm_revenue_usd", LongType)
      .add("dbm_revenue_partner_currency", LongType)
      .add("dbm_revenue_advertiser_currency", LongType)
      .add("dbm_total_media_cost_usd", LongType)
      .add("dbm_total_media_cost_partner_currency", LongType)
      .add("dbm_total_media_cost_advertiser_currency", LongType)
      .add("dbm_cpm_fee1_usd", IntegerType)
      .add("dbm_cpm_fee1_partner_currency", LongType)
      .add("dbm_cpm_fee1_advertiser_currency", LongType)
      .add("dbm_cpm_fee2_usd", LongType)
      .add("dbm_cpm_fee2_partner_currency", LongType)
      .add("dbm_cpm_fee2_advertiser_currency", LongType)
      .add("dbm_cpm_fee3_usd", LongType)
      .add("dbm_cpm_fee3_partner_currency", LongType)
      .add("dbm_cpm_fee3_advertiser_currency", LongType)
      .add("dbm_cpm_fee4_usd", LongType)
      .add("dbm_cpm_fee4_partner_currency", LongType)
      .add("dbm_cpm_fee4_advertiser_currency", LongType)
      .add("dbm_cpm_fee5_usd", LongType)
      .add("dbm_cpm_fee5_partner_currency", LongType)
      .add("dbm_cpm_fee5_advertiser_currency", LongType)
      .add("dbm_media_fee1_usd", LongType)
      .add("dbm_media_fee1_partner_currency", LongType)
      .add("dbm_media_fee1_advertiser_currency", LongType)
      .add("dbm_media_fee2_usd", LongType)
      .add("dbm_media_fee2_partner_currency", LongType)
      .add("dbm_media_fee2_advertiser_currency", LongType)
      .add("dbm_media_fee3_usd", LongType)
      .add("dbm_media_fee3_partner_currency", LongType)
      .add("dbm_media_fee3_advertiser_currency", LongType)
      .add("dbm_media_fee4_usd", LongType)
      .add("dbm_media_fee4_partner_currency", LongType)
      .add("dbm_media_fee4_advertiser_currency", LongType)
      .add("dbm_media_fee5_usd", LongType)
      .add("dbm_media_fee5_partner_currency", LongType)
      .add("dbm_media_fee5_advertiser_currency", LongType)
      .add("dbm_data_fees_usd", LongType)
      .add("dbm_data_fees_partner_currency", LongType)
      .add("dbm_data_fees_advertiser_currency", LongType)
      .add("dbm_billable_cost_usd", LongType)
      .add("dbm_billable_cost_partner_currency", LongType)
      .add("dbm_billable_cost_advertiser_currency", LongType)
      .add("active_view_eligible_impressions", LongType)
      .add("active_view_measurable_impressions", LongType)
      .add("active_view_viewable_impressions", LongType)
    val impression1 = spark.read.option("header", true).schema(schema_impression).csv("/data/hive/rawdb/impression/client=kellogg/file_date="+current_date)
    val impression = impression1.withColumn("operating_system_matchcol", when (col("operating_system_id") > 22, col("operating_system_id")).otherwise(pow(2,impression1.col("operating_system_id"))))

    logger.info("Joining Impression data with match Table Data")

    val impression_prq =impression
      .join(advertisers, impression("advertiser_id") === advertisers("avd"), "left_outer")
      .join(campaigns,impression("campaign_id")===campaigns("cam"), "left_outer")
      .join(ads,impression("ad_id")===ads("adi"), "left_outer")
      .join(creatives,impression("rendering_id")===creatives("ren"), "left_outer")
      .join(sites,impression("site_id_dcm")===sites("sit"), "left_outer")
      .join(placements,impression("placement_id") ===placements("pla"), "left_outer")
      .join(states_mod,impression("state_region")===states_mod("region_matchedcol"), "left_outer")
      .join(browsers,impression("browser_platform_id")===browsers("bro"), "left_outer")
      .join(operating_systems,impression("operating_system_matchcol")===operating_systems("ope"), "left_outer")
      .join(designated_market_areas,impression("designated_market_area_dma_id")===designated_market_areas("des"), "left_outer")
      .join(cities,impression("city_id")===cities("cit"), "left_outer")


    logger.info("Adding additional columns to the impression_prq")
    val impression_prq_DateConverted = impression_prq
      .withColumn("event_time_part", substring(from_unixtime(col("event_time").divide(1000000)),12,8))
      .withColumn("event_date_part", substring(from_unixtime(col("event_time").divide(1000000)),1,10))
      .withColumn("millisecond_part", round(((col("event_time").mod(1000000l)).divide(1000)),0))



    logger.info("Gettting the final Impression_prq data")

    val impression_prq_data = impression_prq_DateConverted.select("event_time"
      ,"event_time_part"
      ,"millisecond_part"
      ,"event_date_part"
      ,"user_id"
      ,"advertiser_id"
      ,"advertiser"
      ,"campaign_id"
      ,"campaign"
      ,"ad_id"
      ,"ad"
      ,"rendering_id"
      ,"creative_id"
      ,"creative"
      ,"creative_version"
      ,"site_id_dcm"
      ,"site_dcm"
      ,"placement_id"
      ,"placement"
      ,"country_code"
      ,"state_region"
      ,"region"
      ,"state_region_full_name"
      ,"browser_platform_id"
      ,"browser_platform"
      ,"browser_platform_version"
      ,"operating_system_id"
      ,"operating_system"
      ,"designated_market_area_dma_id"
      ,"designated_market_area_dma"
      ,"city_id"
      ,"city"
      ,"zip_postal_code"
      ,"u_value"
      ,"event_type"
      ,"event_subtype"
      ,"dbm_auction_id"
      ,"dbm_request_time"
      ,"dbm_advertiser_id"
      ,"dbm_insertion_order_id"
      ,"dbm_line_item_id"
      ,"dbm_creative_id"
      ,"dbm_bid_price_usd"
      ,"dbm_bid_price_partner_currency"
      ,"dbm_bid_price_advertiser_currency"
      ,"dbm_url","dbm_site_id"
      ,"dbm_language"
      ,"dbm_adx_page_categories"
      ,"dbm_matching_targeted_keywords"
      ,"dbm_exchange_id"
      ,"dbm_attributed_inventory_source_external_id"
      ,"dbm_attributed_inventory_source_is_public"
      ,"dbm_ad_position"
      ,"dbm_country_code"
      ,"dbm_designated_market_area_dma_id"
      ,"dbm_zip_postal_code"
      ,"dbm_state_region_id"
      ,"dbm_city_id"
      ,"dbm_operating_system_id"
      ,"dbm_browser_platform_id"
      ,"dbm_browser_timezone_offset_minutes"
      ,"dbm_net_speed"
      ,"dbm_matching_targeted_segments"
      ,"dbm_isp_id","dbm_device_type"
      ,"dbm_mobile_make_id"
      ,"dbm_mobile_model_id"
      ,"dbm_view_state"
      ,"partner1_id"
      ,"partner2_id"
      ,"dbm_media_cost_usd"
      ,"dbm_media_cost_partner_currency"
      ,"dbm_media_cost_advertiser_currency"
      ,"dbm_revenue_usd"
      ,"dbm_revenue_partner_currency"
      ,"dbm_revenue_advertiser_currency"
      ,"dbm_total_media_cost_usd"
      ,"dbm_total_media_cost_partner_currency"
      ,"dbm_total_media_cost_advertiser_currency"
      ,"dbm_cpm_fee1_usd"
      ,"dbm_cpm_fee1_partner_currency"
      ,"dbm_cpm_fee1_advertiser_currency"
      ,"dbm_cpm_fee2_usd"
      ,"dbm_cpm_fee2_partner_currency"
      ,"dbm_cpm_fee2_advertiser_currency"
      ,"dbm_cpm_fee3_usd"
      ,"dbm_cpm_fee3_partner_currency"
      ,"dbm_cpm_fee3_advertiser_currency"
      ,"dbm_cpm_fee4_usd"
      ,"dbm_cpm_fee4_partner_currency"
      ,"dbm_cpm_fee4_advertiser_currency"
      ,"dbm_cpm_fee5_usd"
      ,"dbm_cpm_fee5_partner_currency"
      ,"dbm_cpm_fee5_advertiser_currency"
      ,"dbm_media_fee1_usd"
      ,"dbm_media_fee1_partner_currency"
      ,"dbm_media_fee1_advertiser_currency"
      ,"dbm_media_fee2_usd"
      ,"dbm_media_fee2_partner_currency"
      ,"dbm_media_fee2_advertiser_currency"
      ,"dbm_media_fee3_usd"
      ,"dbm_media_fee3_partner_currency"
      ,"dbm_media_fee3_advertiser_currency"
      ,"dbm_media_fee4_usd"
      ,"dbm_media_fee4_partner_currency"
      ,"dbm_media_fee4_advertiser_currency"
      ,"dbm_media_fee5_usd"
      ,"dbm_media_fee5_partner_currency"
      ,"dbm_media_fee5_advertiser_currency"
      ,"dbm_data_fees_usd"
      ,"dbm_data_fees_partner_currency"
      ,"dbm_data_fees_advertiser_currency"
      ,"dbm_billable_cost_usd"
      ,"dbm_billable_cost_partner_currency"
      ,"dbm_billable_cost_advertiser_currency"
      ,"active_view_eligible_impressions"
      ,"active_view_measurable_impressions"
      ,"active_view_viewable_impressions")

    logger.info("Saving Impression_prq data in the hdfs")

    impression_prq_data.write.mode(SaveMode.Overwrite).parquet("/data/hive/rawdb/impression_prq/client=kellogg/file_date="+current_date)

      }

}
