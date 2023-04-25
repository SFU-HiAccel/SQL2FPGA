package org.example
import org.apache.spark.sql.{DataFrame, SparkSession}

// TPC-DS table schemas

case class Customer_tpcds(
                           c_customer_sk           : Int, //bigint
                           c_customer_id           : String,
                           c_current_cdemo_sk      : Int, //bigint
                           c_current_hdemo_sk      : Int, //bigint
                           c_current_addr_sk       : Int, //bigint
                           c_first_shipto_date_sk  : Int, //bigint
                           c_first_sales_date_sk   : Int, //bigint
                           c_salutation            : String,
                           c_first_name            : String,
                           c_last_name             : String,
                           c_preferred_cust_flag   : String,
                           c_birth_day             : Int,
                           c_birth_month           : Int,
                           c_birth_year            : Int,
                           c_birth_country         : String,
                           c_login                 : String,
                           c_email_address         : String,
                           c_last_review_date      : String
                         )

case class Customer_address(
                                   ca_address_sk           : Int, //bigint
                                   ca_address_id           : String,
                                   ca_street_number        : String,
                                   ca_street_name          : String,
                                   ca_street_type          : String,
                                   ca_suite_number         : String,
                                   ca_city                 : String,
                                   ca_county               : String,
                                   ca_state                : String,
                                   ca_zip                  : String,
                                   ca_country              : String,
                                   ca_gmt_offset           : Int, //decimal(5,2)
                                   ca_location_type        : String
                                 )

case class Customer_demographics(
                                        cd_demo_sk              : Int, //bigint
                                        cd_gender               : String,
                                        cd_marital_status       : String,
                                        cd_education_status     : String,
                                        cd_purchase_estimate    : Int,
                                        cd_credit_rating        : String,
                                        cd_dep_count            : Int,
                                        cd_dep_employed_count   : Int,
                                        cd_dep_college_count    : Int
                                      )

case class Date_dim(
                           d_date_sk           : Int, //bigint
                           d_date_id           : String,
                           d_date              : Int,
                           d_month_seq         : Int,
                           d_week_seq          : Int,
                           d_quarter_seq       : Int,
                           d_year              : Int,
                           d_dow               : Int,
                           d_moy               : Int,
                           d_dom               : Int,
                           d_qoy               : Int,
                           d_fy_year           : Int,
                           d_fy_quarter_seq    : Int,
                           d_fy_week_seq       : Int,
                           d_day_name          : String,
                           d_quarter_name      : String,
                           d_holiday           : String,
                           d_weekend           : String,
                           d_following_holiday : String,
                           d_first_dom         : Int,
                           d_last_dom          : Int,
                           d_same_day_ly       : Int,
                           d_same_day_lq       : Int,
                           d_current_day       : String,
                           d_current_week      : String,
                           d_current_month     : String,
                           d_current_quarter   : String,
                           d_current_year      : String
                         )

case class Household_demographics(
                                         hd_demo_sk          : Int, //bigint
                                         hd_income_band_sk   : Int, //bigint
                                         hd_buy_potential    : String,
                                         hd_dep_count        : Int,
                                         hd_vehicle_count    : Int
                                       )

case class tpcds_income_band(
                              ib_income_band_sk   : Int, //bigint
                              ib_lower_bound      : Int,
                              ib_upper_bound      : Int
                            )

case class Item(
                       i_item_sk       : Int, //bigint,
                       i_item_id       : String,
                       i_rec_start_date: String,
                       i_rec_end_date  : String,
                       i_item_desc     : String,
                       i_current_price : Int, //decimal(7,2)
                       i_wholesale_cost: Int, //decimal(7,2)
                       i_brand_id      : Int,
                       i_brand         : String,
                       i_class_id      : Int,
                       i_class         : String,
                       i_category_id   : Int,
                       i_category      : String,
                       i_manufact_id   : Int,
                       i_manufact      : String,
                       i_size          : String,
                       i_formulation   : String,
                       i_color         : String,
                       i_units         : String,
                       i_container     : String,
                       i_manager_id    : Int,
                       i_product_name  : String
                     )

case class Promotion(
                            p_promo_sk              : Int, //bigint
                            p_promo_id              : String,
                            p_start_date_sk         : Int, //bigint
                            p_end_date_sk           : Int, //bigint
                            p_item_sk               : Int, //bigint
                            p_cost                  : Int, //decimal(15,2)
                            p_response_target       : Int,
                            p_promo_name            : String,
                            p_channel_dmail         : String,
                            p_channel_email         : String,
                            p_channel_catalog       : String,
                            p_channel_tv            : String,
                            p_channel_radio         : String,
                            p_channel_press         : String,
                            p_channel_event         : String,
                            p_channel_demo          : String,
                            p_channel_details       : String,
                            p_purpose               : String,
                            p_discount_active       : String
                          )

case class tpcds_reason(
                         r_reason_sk     : Int, //bigint
                         r_reason_id     : String,
                         r_reason_desc   : String
                       )

case class tpcds_ship_mode(
                            sm_ship_mode_sk : Int, //bigint
                            sm_ship_mode_id : String,
                            sm_type         : String,
                            sm_code         : String,
                            sm_carrier      : String,
                            sm_contract     : String
                          )

case class Store(
                        s_store_sk          : Int, //bigint
                        s_store_id          : String,
                        s_rec_start_date    : String,
                        s_rec_end_date      : String,
                        s_closed_date_sk    : Int, //bigint,
                        s_store_name        : String,
                        s_number_employees  : Int ,
                        s_floor_space       : Int ,
                        s_hours             : String ,
                        s_manager           : String ,
                        s_market_id         : Int ,
                        s_geography_class   : String ,
                        s_market_desc       : String ,
                        s_market_manager    : String ,
                        s_division_id       : Int ,
                        s_division_name     : String ,
                        s_company_id        : Int ,
                        s_company_name      : String,
                        s_street_number     : String,
                        s_street_name       : String,
                        s_street_type       : String,
                        s_suite_number      : String,
                        s_city              : String,
                        s_county            : String,
                        s_state             : String,
                        s_zip               : String,
                        s_country           : String,
                        s_gmt_offset        : Int, //decimal(5,2)
                        s_tax_precentage    : Int  //decimal(5,2)
                      )

case class Time_dim(
                           t_time_sk   : Int, //bigint
                           t_time_id   : String,
                           t_time      : Int,
                           t_hour      : Int,
                           t_minute    : Int,
                           t_second    : Int,
                           t_am_pm     : String,
                           t_shift     : String,
                           t_sub_shift : String,
                           t_meal_time : String
                         )

case class tpcds_warehouse(
                            w_warehouse_sk      : Int, //bigint
                            w_warehouse_id      : String ,
                            w_warehouse_name    : String ,
                            w_warehouse_sq_ft   : Int ,
                            w_street_number     : String ,
                            w_street_name       : String ,
                            w_street_type       : String ,
                            w_suite_number      : String ,
                            w_city              : String ,
                            w_county            : String ,
                            w_state             : String ,
                            w_zip               : String ,
                            w_country           : String ,
                            w_gmt_offset        : Int    //decimal(5,2)
                          )

  case class tpcds_web_site(
                             web_site_sk         : Int , //bigint
                             web_site_id         : String ,
                             web_rec_start_date  : String ,
                             web_rec_end_date    : String ,
                             web_name            : String ,
                             web_open_date_sk    : Int , //bigint
                             web_close_date_sk   : Int , //bigint
                             web_class           : String ,
                             web_manager         : String ,
                             web_mkt_id          : Int ,
                             web_mkt_class       : String ,
                             web_mkt_desc        : String ,
                             web_market_manager  : String ,
                             web_company_id      : Int ,
                             web_company_name    : String ,
                             web_street_number   : String ,
                             web_street_name     : String ,
                             web_street_type     : String ,
                             web_suite_number    : String ,
                             web_city            : String ,
                             web_county          : String ,
                             web_state           : String ,
                             web_zip             : String ,
                             web_country         : String ,
                             web_gmt_offset      : Int, //decimal(5,2) ,
                             web_tax_percentage  : Int  //decimal(5,2)
                           )

  case class tpcds_web_page(
                             wp_web_page_sk      : Int, //bigint
                             wp_web_page_id      : String ,
                             wp_rec_start_date   : String ,
                             wp_rec_end_date     : String ,
                             wp_creation_date_sk : Int, //bigint
                             wp_access_date_sk   : Int, //bigint
                             wp_autogen_flag     : String ,
                             wp_customer_sk      : Int, //bigint
                             wp_url              : String ,
                             wp_type             : String ,
                             wp_char_count       : Int ,
                             wp_link_count       : Int ,
                             wp_image_count      : Int ,
                             wp_max_ad_count     : Int
                           )

  case class tpcds_inventory(
                              inv_date_sk             : Int, //bigint
                              inv_item_sk             : Int, //bigint
                              inv_warehouse_sk        : Int, //bigint
                              inv_quantity_on_hand    : Int
                            )

  case class Store_returns(
                                  sr_returned_date_sk     : Int , //bigint
                                  sr_return_time_sk       : Int , //bigint
                                  sr_item_sk              : Int , //bigint
                                  sr_customer_sk          : Int , //bigint
                                  sr_cdemo_sk             : Int , //bigint
                                  sr_hdemo_sk             : Int , //bigint
                                  sr_addr_sk              : Int , //bigint
                                  sr_store_sk             : Int , //bigint
                                  sr_reason_sk            : Int , //bigint
                                  sr_ticket_number        : Int , //bigint
                                  sr_return_quantity      : Int ,
                                  sr_return_amt           : Int , //decimal(7,2)
                                  sr_return_tax           : Int , //decimal(7,2)
                                  sr_return_amt_inc_tax   : Int , //decimal(7,2)
                                  sr_fee                  : Int , //decimal(7,2)
                                  sr_return_ship_cost     : Int , //decimal(7,2)
                                  sr_refunded_cash        : Int , //decimal(7,2)
                                  sr_reversed_charge      : Int , //decimal(7,2)
                                  sr_store_credit         : Int , //decimal(7,2)
                                  sr_net_loss             : Int    //decimal(7,2)
                                )

  case class tpcds_web_sales(
                              ws_sold_date_sk             : Int, //bigint
                              ws_sold_time_sk             : Int, //bigint
                              ws_ship_date_sk             : Int, //bigint
                              ws_item_sk                  : Int, //bigint
                              ws_bill_customer_sk         : Int, //bigint
                              ws_bill_cdemo_sk            : Int, //bigint
                              ws_bill_hdemo_sk            : Int, //bigint
                              ws_bill_addr_sk             : Int, //bigint
                              ws_ship_customer_sk         : Int, //bigint
                              ws_ship_cdemo_sk            : Int, //bigint
                              ws_ship_hdemo_sk            : Int, //bigint
                              ws_ship_addr_sk             : Int, //bigint
                              ws_web_page_sk              : Int, //bigint
                              ws_web_site_sk              : Int, //bigint
                              ws_ship_mode_sk             : Int, //bigint
                              ws_warehouse_sk             : Int, //bigint
                              ws_promo_sk                 : Int, //bigint
                              ws_order_number             : Int, //bigint
                              ws_quantity                 : Int ,
                              ws_wholesale_cost           : Int , //decimal(7,2)
                              ws_list_price               : Int , //decimal(7,2)
                              ws_sales_price              : Int , //decimal(7,2)
                              ws_ext_discount_amt         : Int , //decimal(7,2)
                              ws_ext_sales_price          : Int , //decimal(7,2)
                              ws_ext_wholesale_cost       : Int , //decimal(7,2)
                              ws_ext_list_price           : Int , //decimal(7,2)
                              ws_ext_tax                  : Int , //decimal(7,2)
                              ws_coupon_amt               : Int , //decimal(7,2)
                              ws_ext_ship_cost            : Int , //decimal(7,2)
                              ws_net_paid                 : Int , //decimal(7,2)
                              ws_net_paid_inc_tax         : Int , //decimal(7,2)
                              ws_net_paid_inc_ship        : Int , //decimal(7,2)
                              ws_net_paid_inc_ship_tax    : Int , //decimal(7,2)
                              ws_net_profit               : Int   //decimal(7,2)
                            )

  case class tpcds_web_returns(
                                wr_returned_date_sk     : Int , //bigint
                                wr_returned_time_sk     : Int , //bigint
                                wr_item_sk              : Int , //bigint
                                wr_refunded_customer_sk : Int , //bigint
                                wr_refunded_cdemo_sk    : Int , //bigint
                                wr_refunded_hdemo_sk    : Int , //bigint
                                wr_refunded_addr_sk     : Int , //bigint
                                wr_returning_customer_sk: Int , //bigint
                                wr_returning_cdemo_sk   : Int , //bigint
                                wr_returning_hdemo_sk   : Int , //bigint
                                wr_returning_addr_sk    : Int , //bigint
                                wr_web_page_sk          : Int , //bigint
                                wr_reason_sk            : Int , //bigint
                                wr_order_number         : Int , //bigint
                                wr_return_quantity      : Int ,
                                wr_return_amt           : Int , //decimal(7,2)
                                wr_return_tax           : Int , //decimal(7,2)
                                wr_return_amt_inc_tax   : Int , //decimal(7,2)
                                wr_fee                  : Int , //decimal(7,2)
                                wr_return_ship_cost     : Int , //decimal(7,2)
                                wr_refunded_cash        : Int , //decimal(7,2)
                                wr_reversed_charge      : Int , //decimal(7,2)
                                wr_account_credit       : Int , //decimal(7,2)
                                wr_net_loss             : Int   //decimal(7,2)
                              )

  case class tpcds_call_center(
                                cc_call_center_sk   : Int , //bigint
                                cc_call_center_id   : String,
                                cc_rec_start_date   : String,
                                cc_rec_end_date     : String,
                                cc_closed_date_sk   : Int ,
                                cc_open_date_sk     : Int ,
                                cc_name             : String,
                                cc_class            : String,
                                cc_employees        : Int ,
                                cc_sq_ft            : Int ,
                                cc_hours            : String,
                                cc_manager          : String,
                                cc_mkt_id           : Int ,
                                cc_mkt_class        : String,
                                cc_mkt_desc         : String,
                                cc_market_manager   : String,
                                cc_division         : Int ,
                                cc_division_name    : String,
                                cc_company          : Int ,
                                cc_company_name     : String,
                                cc_street_number    : String,
                                cc_street_name      : String,
                                cc_street_type      : String,
                                cc_suite_number     : String,
                                cc_city             : String,
                                cc_county           : String,
                                cc_state            : String,
                                cc_zip              : String,
                                cc_country          : String,
                                cc_gmt_offset       : Int , //decimal(5,2)
                                cc_tax_percentage   : Int   //decimal(5,2)
                              )

  case class tpcds_catalog_page(
                                 cp_catalog_page_sk      : Int , //bigint
                                 cp_catalog_page_id      : String,
                                 cp_start_date_sk        : Int ,
                                 cp_end_date_sk          : Int ,
                                 cp_department           : String,
                                 cp_catalog_number       : Int ,
                                 cp_catalog_page_number  : Int ,
                                 cp_description          : String,
                                 cp_type                 : String
                               )

  case class tpcds_catalog_returns(
                                    cr_returned_date_sk         : Int, //bigint ,
                                    cr_returned_time_sk         : Int, //bigint ,
                                    cr_item_sk                  : Int, //bigint ,
                                    cr_refunded_customer_sk     : Int, //bigint ,
                                    cr_refunded_cdemo_sk        : Int, //bigint ,
                                    cr_refunded_hdemo_sk        : Int, //bigint ,
                                    cr_refunded_addr_sk         : Int, //bigint ,
                                    cr_returning_customer_sk    : Int, //bigint,
                                    cr_returning_cdemo_sk       : Int, //bigint,
                                    cr_returning_hdemo_sk       : Int, //bigint,
                                    cr_returning_addr_sk        : Int, //bigint ,
                                    cr_call_center_sk           : Int, //bigint ,
                                    cr_catalog_page_sk          : Int, //bigint ,
                                    cr_ship_mode_sk             : Int, //bigint ,
                                    cr_warehouse_sk             : Int, //bigint ,
                                    cr_reason_sk                : Int, //bigint ,
                                    cr_order_number             : Int, //bigint ,
                                    cr_return_quantity          : Int,
                                    cr_return_amount            : Int, //decimal(7,2),
                                    cr_return_tax               : Int, //decimal(7,2),
                                    cr_return_amt_inc_tax       : Int, //decimal(7,2),
                                    cr_fee                      : Int, //decimal(7,2),
                                    cr_return_ship_cost         : Int, //decimal(7,2),
                                    cr_refunded_cash            : Int, //decimal(7,2),
                                    cr_reversed_charge          : Int, //decimal(7,2),
                                    cr_store_credit             : Int, //decimal(7,2),
                                    cr_net_loss                 : Int  //decimal(7,2)
                                  )

  case class Catalog_sales(
                                  cs_sold_date_sk             : Int, //bigint ,
                                  cs_sold_time_sk             : Int, //bigint ,
                                  cs_ship_date_sk             : Int, //bigint ,
                                  cs_bill_customer_sk         : Int, //bigint ,
                                  cs_bill_cdemo_sk            : Int, //bigint ,
                                  cs_bill_hdemo_sk            : Int, //bigint ,
                                  cs_bill_addr_sk             : Int, //bigint ,
                                  cs_ship_customer_sk         : Int, //bigint ,
                                  cs_ship_cdemo_sk            : Int, //bigint ,
                                  cs_ship_hdemo_sk            : Int, //bigint ,
                                  cs_ship_addr_sk             : Int, //bigint ,
                                  cs_call_center_sk           : Int, //bigint ,
                                  cs_catalog_page_sk          : Int, //bigint ,
                                  cs_ship_mode_sk             : Int, //bigint ,
                                  cs_warehouse_sk             : Int, //bigint ,
                                  cs_item_sk                  : Int, //bigint ,
                                  cs_promo_sk                 : Int, //bigint ,
                                  cs_order_number             : Int, //bigint ,
                                  cs_quantity                 : Int,
                                  cs_wholesale_cost           : Int, //decimal(7,2),
                                  cs_list_price               : Int, //decimal(7,2),
                                  cs_sales_price              : Int, //decimal(7,2),
                                  cs_ext_discount_amt         : Int, //decimal(7,2),
                                  cs_ext_sales_price          : Int, //decimal(7,2),
                                  cs_ext_wholesale_cost       : Int, //decimal(7,2),
                                  cs_ext_list_price           : Int, //decimal(7,2),
                                  cs_ext_tax                  : Int, //decimal(7,2),
                                  cs_coupon_amt               : Int, //decimal(7,2),
                                  cs_ext_ship_cost            : Int, //decimal(7,2),
                                  cs_net_paid                 : Int, //decimal(7,2),
                                  cs_net_paid_inc_tax         : Int, //decimal(7,2),
                                  cs_net_paid_inc_ship        : Int, //decimal(7,2),
                                  cs_net_paid_inc_ship_tax    : Int, //decimal(7,2),
                                  cs_net_profit               : Int  //decimal(7,2)
                                )

  case class Store_sales(
                                ss_sold_date_sk           : Int, //bigint,
                                ss_sold_time_sk           : Int, //bigint,
                                ss_item_sk                : Int, //bigint,
                                ss_customer_sk            : Int, //bigint,
                                ss_cdemo_sk               : Int, //bigint,
                                ss_hdemo_sk               : Int, //bigint,
                                ss_addr_sk                : Int, //bigint,
                                ss_store_sk               : Int, //bigint,
                                ss_promo_sk               : Int, //bigint,
                                ss_ticket_number          : Int, //bigint,
                                ss_quantity               : Int,
                                ss_wholesale_cost         : Int, //decimal(7,2),
                                ss_list_price             : Int, //decimal(7,2),
                                ss_sales_price            : Int, //decimal(7,2),
                                ss_ext_discount_amt       : Int, //decimal(7,2),
                                ss_ext_sales_price        : Int, //decimal(7,2),
                                ss_ext_wholesale_cost     : Int, //decimal(7,2),
                                ss_ext_list_price         : Int, //decimal(7,2),
                                ss_ext_tax                : Int, //decimal(7,2),
                                ss_coupon_amt             : Int, //decimal(7,2),
                                ss_net_paid               : Int, //decimal(7,2),
                                ss_net_paid_inc_tax       : Int, //decimal(7,2),
                                ss_net_profit             : Int  //decimal(7,2)
                              )

class TpcdsSchemaProvider(sc: SparkSession, inputDir: String) {
  import sc.implicits._

  val dfMap = Map(
    "customer" -> sc.read.textFile(inputDir + "/customer.dat*").map(_.split('|')).map(p =>
      Customer_tpcds(p(0).trim.toInt, p(1).trim, p(2).trim.toInt, p(3).trim.toInt, p(4).trim.toInt, p(5).trim.toInt, p(6).trim.toInt, p(7).trim, p(8).trim, p(9).trim, p(10).trim, p(11).trim.toInt, p(12).trim.toInt, p(13).trim.toInt, p(14).trim, p(15).trim, p(16).trim, p(17).trim)).toDF(),

    "customer_address" -> sc.read.textFile(inputDir + "/customer_address.dat*").map(_.split('|')).map(p =>
      Customer_address(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim, p(4).trim, p(5).trim, p(6).trim, p(7).trim, p(8).trim, p(9).trim, p(10).trim, (p(11).trim.toDouble*100).toInt, p(12).trim)).toDF(),

    "customer_demographics" -> sc.read.textFile(inputDir + "/customer_demographics.dat*").map(_.split('|')).map(p =>
      Customer_demographics(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim, p(4).trim.toInt, p(5).trim, p(6).trim.toInt, p(7).trim.toInt, p(8).trim.toInt)).toDF(),

    "date_dim" -> sc.read.textFile(inputDir + "/date_dim.dat*").map(_.split('|')).map(p =>
      Date_dim(p(0).trim.toInt, p(1).trim, p(2).trim.replace("-", "").toInt, p(3).trim.toInt, p(4).trim.toInt, p(5).trim.toInt, p(6).trim.toInt, p(7).trim.toInt, p(8).trim.toInt, p(9).trim.toInt, p(10).trim.toInt, p(11).trim.toInt, p(12).trim.toInt, p(13).trim.toInt, p(14).trim, p(15).trim, p(16).trim, p(17).trim, p(18).trim, p(19).trim.toInt, p(20).trim.toInt, p(21).trim.toInt, p(22).trim.toInt, p(23).trim, p(24).trim, p(25).trim, p(26).trim, p(27).trim)).toDF(),

    "household_demographics" -> sc.read.textFile(inputDir + "/household_demographics.dat*").map(_.split('|')).map(p =>
      Household_demographics(p(0).trim.toInt, p(1).trim.toInt, p(2).trim, p(3).trim.toInt, p(4).trim.toInt)).toDF(),

    "income_band" -> sc.read.textFile(inputDir + "/income_band.dat*").map(_.split('|')).map(p =>
      tpcds_income_band(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt)).toDF(),

    "item" -> sc.read.textFile(inputDir + "/item.dat*").map(_.split('|')).map(p =>
      Item(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim, p(4).trim, (p(5).trim.toDouble*100).toInt, (p(6).trim.toDouble*100).toInt, p(7).trim.toInt, p(8).trim, p(9).trim.toInt, p(10).trim, p(11).trim.toInt, p(12).trim, p(13).trim.toInt, p(14).trim, p(15).trim, p(16).trim, p(17).trim, p(18).trim, p(19).trim, p(20).trim.toInt, p(21).trim)).toDF(),

    "promotion" -> sc.read.textFile(inputDir + "/promotion.dat*").map(_.split('|')).map(p =>
      Promotion(p(0).trim.toInt, p(1).trim, p(2).trim.toInt, p(3).trim.toInt, p(4).trim.toInt, (p(5).trim.toDouble*100).toInt, p(6).trim.toInt, p(7).trim, p(8).trim, p(9).trim, p(10).trim, p(11).trim, p(12).trim, p(13).trim, p(14).trim, p(15).trim, p(16).trim, p(17).trim, p(18).trim)).toDF(),

    "reason" -> sc.read.textFile(inputDir + "/reason.dat*").map(_.split('|')).map(p =>
      tpcds_reason(p(0).trim.toInt, p(1).trim, p(2).trim)).toDF(),

    "ship_mode" -> sc.read.textFile(inputDir + "/ship_mode.dat*").map(_.split('|')).map(p =>
      tpcds_ship_mode(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim, p(4).trim, p(5).trim)).toDF(),

    "store" -> sc.read.textFile(inputDir + "/store.dat*").map(_.split('|')).map(p =>
      Store(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim, p(4).trim.toInt, p(5).trim, p(6).trim.toInt, p(7).trim.toInt, p(8).trim, p(9).trim, p(10).trim.toInt, p(11).trim, p(12).trim, p(13).trim, p(14).trim.toInt, p(15).trim, p(16).trim.toInt, p(17).trim, p(18).trim, p(19).trim, p(20).trim, p(21).trim, p(22).trim, p(23).trim, p(24).trim, p(25).trim, p(26).trim, (p(27).trim.toDouble*100).toInt, (p(28).trim.toDouble*100).toInt)).toDF(),

    "time_dim" -> sc.read.textFile(inputDir + "/time_dim.dat*").map(_.split('|')).map(p =>
      Time_dim(p(0).trim.toInt, p(1).trim, p(2).trim.toInt, p(3).trim.toInt, p(4).trim.toInt, p(5).trim.toInt, p(6).trim, p(7).trim, p(8).trim, p(9).trim)).toDF(),

    "warehouse" -> sc.read.textFile(inputDir + "/warehouse.dat*").map(_.split('|')).map(p =>
      tpcds_warehouse(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim.toInt, p(4).trim, p(5).trim, p(6).trim, p(7).trim, p(8).trim, p(9).trim, p(10).trim, p(11).trim, p(12).trim, (p(13).trim.toDouble*100).toInt)).toDF(),

    "web_site" -> sc.read.textFile(inputDir + "/web_site.dat*").map(_.split('|')).map(p =>
      tpcds_web_site(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim, p(4).trim, p(5).trim.toInt, p(6).trim.toInt, p(7).trim, p(8).trim, p(9).trim.toInt, p(10).trim, p(11).trim, p(12).trim, p(13).trim.toInt, p(14).trim, p(15).trim, p(16).trim, p(17).trim, p(18).trim, p(19).trim, p(20).trim, p(21).trim, p(22).trim, p(23).trim, (p(24).trim.toDouble*100).toInt, (p(25).trim.toDouble*100).toInt)).toDF(),

    "web_page" -> sc.read.textFile(inputDir + "/web_page.dat*").map(_.split('|')).map(p =>
      tpcds_web_page(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim, p(4).trim.toInt, p(5).trim.toInt, p(6).trim, p(7).trim.toInt, p(8).trim, p(9).trim, p(10).trim.toInt, p(11).trim.toInt, p(12).trim.toInt, p(13).trim.toInt)).toDF(),

    "inventory" -> sc.read.textFile(inputDir + "/inventory.dat*").map(_.split('|')).map(p =>
      tpcds_inventory(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toInt)).toDF(),

    "store_returns" -> sc.read.textFile(inputDir + "/store_returns.dat*").map(_.split('|')).map(p =>
      Store_returns(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toInt, p(4).trim.toInt, p(5).trim.toInt, p(6).trim.toInt, p(7).trim.toInt, p(8).trim.toInt, p(9).trim.toInt, p(10).trim.toInt, (p(11).trim.toDouble*100).toInt, (p(12).trim.toDouble*100).toInt, (p(13).trim.toDouble*100).toInt, (p(14).trim.toDouble*100).toInt, (p(15).trim.toDouble*100).toInt, (p(16).trim.toDouble*100).toInt, (p(17).trim.toDouble*100).toInt, (p(18).trim.toDouble*100).toInt, (p(19).trim.toDouble*100).toInt)).toDF(),

    "web_sales" -> sc.read.textFile(inputDir + "/web_sales.dat*").map(_.split('|')).map(p =>
      tpcds_web_sales(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toInt, p(4).trim.toInt, p(5).trim.toInt, p(6).trim.toInt, p(7).trim.toInt, p(8).trim.toInt, p(9).trim.toInt, p(10).trim.toInt, p(11).trim.toInt, p(12).trim.toInt, p(13).trim.toInt, p(14).trim.toInt, p(15).trim.toInt, p(16).trim.toInt, p(17).trim.toInt, p(18).trim.toInt, (p(19).trim.toDouble*100).toInt, (p(20).trim.toDouble*100).toInt, (p(21).trim.toDouble*100).toInt, (p(22).trim.toDouble*100).toInt, (p(23).trim.toDouble*100).toInt, (p(24).trim.toDouble*100).toInt, (p(25).trim.toDouble*100).toInt, (p(26).trim.toDouble*100).toInt, (p(27).trim.toDouble*100).toInt, (p(28).trim.toDouble*100).toInt, (p(29).trim.toDouble*100).toInt, (p(30).trim.toDouble*100).toInt, (p(31).trim.toDouble*100).toInt, (p(32).trim.toDouble*100).toInt, (p(33).trim.toDouble*100).toInt)).toDF(),

    "web_returns" -> sc.read.textFile(inputDir + "/web_returns.dat*").map(_.split('|')).map(p =>
      tpcds_web_returns(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toInt, p(4).trim.toInt, p(5).trim.toInt, p(6).trim.toInt, p(7).trim.toInt, p(8).trim.toInt, p(9).trim.toInt, p(10).trim.toInt, p(11).trim.toInt, p(12).trim.toInt, p(13).trim.toInt, p(14).trim.toInt, (p(15).trim.toDouble*100).toInt, (p(16).trim.toDouble*100).toInt, (p(17).trim.toDouble*100).toInt, (p(18).trim.toDouble*100).toInt, (p(19).trim.toDouble*100).toInt, (p(20).trim.toDouble*100).toInt, (p(21).trim.toDouble*100).toInt, (p(22).trim.toDouble*100).toInt, (p(23).trim.toDouble*100).toInt)).toDF(),

    "call_center" -> sc.read.textFile(inputDir + "/call_center.dat*").map(_.split('|')).map(p =>
      tpcds_call_center(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim, p(4).trim.toInt, p(5).trim.toInt, p(6).trim, p(7).trim, p(8).trim.toInt, p(9).trim.toInt, p(10).trim, p(11).trim, p(12).trim.toInt, p(13).trim, p(14).trim, p(15).trim, p(16).trim.toInt, p(17).trim, p(18).trim.toInt, p(19).trim, p(20).trim, p(21).trim, p(22).trim, p(23).trim, p(24).trim, p(25).trim, p(26).trim, p(27).trim, p(28).trim, (p(29).trim.toDouble*100).toInt, (p(30).trim.toDouble*100).toInt)).toDF(),

    "catalog_page" -> sc.read.textFile(inputDir + "/catalog_page.dat*").map(_.split('|')).map(p =>
      tpcds_catalog_page(p(0).trim.toInt, p(1).trim, p(2).trim.toInt, p(3).trim.toInt, p(4).trim, p(5).trim.toInt, p(6).trim.toInt, p(7).trim, p(8).trim)).toDF(),

    "catalog_returns" -> sc.read.textFile(inputDir + "/catalog_returns.dat*").map(_.split('|')).map(p =>
      tpcds_catalog_returns(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toInt, p(4).trim.toInt, p(5).trim.toInt, p(6).trim.toInt, p(7).trim.toInt, p(8).trim.toInt, p(9).trim.toInt, p(10).trim.toInt, p(11).trim.toInt, p(12).trim.toInt, p(13).trim.toInt, p(14).trim.toInt, p(15).trim.toInt, p(16).trim.toInt, p(17).trim.toInt, (p(18).trim.toDouble*100).toInt, (p(19).trim.toDouble*100).toInt, (p(20).trim.toDouble*100).toInt, (p(21).trim.toDouble*100).toInt, (p(22).trim.toDouble*100).toInt, (p(23).trim.toDouble*100).toInt, (p(24).trim.toDouble*100).toInt, (p(25).trim.toDouble*100).toInt, (p(26).trim.toDouble*100).toInt)).toDF(),

    "catalog_sales" -> sc.read.textFile(inputDir + "/catalog_sales.dat*").map(_.split('|')).map(p =>
      Catalog_sales(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toInt, p(4).trim.toInt, p(5).trim.toInt, p(6).trim.toInt, p(7).trim.toInt, p(8).trim.toInt, p(9).trim.toInt, p(10).trim.toInt, p(11).trim.toInt, p(12).trim.toInt, p(13).trim.toInt, p(14).trim.toInt, p(15).trim.toInt, p(16).trim.toInt, p(17).trim.toInt, p(18).trim.toInt, (p(19).trim.toDouble*100).toInt, (p(20).trim.toDouble*100).toInt, (p(21).trim.toDouble*100).toInt, (p(22).trim.toDouble*100).toInt, (p(23).trim.toDouble*100).toInt, (p(24).trim.toDouble*100).toInt, (p(25).trim.toDouble*100).toInt, (p(26).trim.toDouble*100).toInt, (p(27).trim.toDouble*100).toInt, (p(28).trim.toDouble*100).toInt, (p(29).trim.toDouble*100).toInt, (p(30).trim.toDouble*100).toInt, (p(31).trim.toDouble*100).toInt, (p(32).trim.toDouble*100).toInt, (p(33).trim.toDouble*100).toInt)).toDF(),

    "store_sales" -> sc.read.textFile(inputDir + "/store_sales.dat*").map(_.split('|')).map(p =>
      Store_sales(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toInt, p(4).trim.toInt, p(5).trim.toInt, p(6).trim.toInt, p(7).trim.toInt, p(8).trim.toInt, p(9).trim.toInt, p(10).trim.toInt, (p(11).trim.toDouble*100).toInt, (p(12).trim.toDouble*100).toInt, (p(13).trim.toDouble*100).toInt, (p(14).trim.toDouble*100).toInt, (p(15).trim.toDouble*100).toInt, (p(16).trim.toDouble*100).toInt, (p(17).trim.toDouble*100).toInt, (p(18).trim.toDouble*100).toInt, (p(19).trim.toDouble*100).toInt, (p(20).trim.toDouble*100).toInt, (p(21).trim.toDouble*100).toInt, (p(22).trim.toDouble*100).toInt)).toDF()
  )

  // for implicits
  val customer: DataFrame = dfMap("customer")
  val customer_address: DataFrame = dfMap("customer_address")
  val customer_demographics: DataFrame = dfMap("customer_demographics")
  val date_dim: DataFrame = dfMap("date_dim")
  val household_demographics: DataFrame = dfMap("household_demographics")
  val income_band: DataFrame = dfMap("income_band")
  val item: DataFrame = dfMap("item")
  val promotion: DataFrame = dfMap("promotion")
  val reason: DataFrame = dfMap("reason")
  val ship_mode: DataFrame = dfMap("ship_mode")
  val store: DataFrame = dfMap("store")
  val time_dim: DataFrame = dfMap("time_dim")
  val warehouse: DataFrame = dfMap("warehouse")
  val web_site: DataFrame = dfMap("web_site")
  val web_page: DataFrame = dfMap("web_page")
  val inventory: DataFrame = dfMap("inventory")
  val store_returns: DataFrame = dfMap("store_returns")
  val web_sales: DataFrame = dfMap("web_sales")
  val web_returns: DataFrame = dfMap("web_returns")
  val call_center: DataFrame = dfMap("call_center")
  val catalog_page: DataFrame = dfMap("catalog_page")
  val catalog_returns: DataFrame = dfMap("catalog_returns")
  val catalog_sales: DataFrame = dfMap("catalog_sales")
  val store_sales: DataFrame = dfMap("store_sales")

  dfMap.foreach {
    case (key, value) => value.createOrReplaceTempView(key)
  }
}