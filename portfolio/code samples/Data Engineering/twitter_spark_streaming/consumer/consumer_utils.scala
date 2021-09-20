// Databricks notebook source
// DBTITLE 1,Consumer Utils
object consumerUtils extends Serializable {
  
  //Twitter fields
  val twitterUserFields = Array("id", "id_str", "name", "screen_name", "location", "url",
                                "description", "translator_type", "protected", "verified",
                                "followers_count", "friends_count", "listed_count", "favourites_count", 
                                "statuses_count", "created_at", "utc_offset", "time_zone", "geo_enabled", 
                                "lang", "contributors_enabled", "is_translator", "profile_background_color", 
                                "profile_background_image_url", "profile_background_image_url_https", 
                                "profile_background_tile", "profile_link_color", "profile_sidebar_border_color", 
                                "profile_sidebar_fill_color", "profile_text_color", "profile_use_background_image", 
                                "profile_image_url", "profile_image_url_https", "profile_banner_url", "default_profile", 
                                "default_profile_image", "following", "follow_request_sent", "notifications") 
  
  val twitterTweetFields =  Array("created_at", "id", "id_str", "text", "source", "truncated", "in_reply_to_status_id",
                                  "in_reply_to_status_id_str", "in_reply_to_user_id", "in_reply_to_user_id_str", 
                                  "in_reply_to_screen_name", "user", "geo", "coordinates", "place", "contributors",
                                  "retweeted_status", "is_quote_status", "quote_count", "reply_count", "retweet_count",
                                  "favorite_count", "entities", "favorited", "retweeted", "filter_level", "lang", "timestamp_ms")
  
  val twitterPlaceFields = Array("id", "url", "place_type", "name", "full_name", "country_code", "country", "bounding_box", "attributes")
  
  //Structuring methods
  private def getStructFromFields(fields:Array[String]): StructType = {
    val structFields = fields.map(fieldName => StructField(fieldName, StringType, nullable = true))
    val dataSchema = StructType(structFields)
    return dataSchema
  }
    
  def getTweetStruct(): StructType =  getStructFromFields(twitterTweetFields)
  def getUserStruct():StructType = getStructFromFields(twitterUserFields)
  def getPlaceStruct(): StructType = getStructFromFields(twitterPlaceFields)
  
  //datetime
  private def sf = new SimpleDateFormat( "EEE MMM dd HH:mm:ss ZZZZZ yyyy", Locale.ENGLISH)
  sf.setLenient(true)

  private def parseDatetimeString(dateStr: String): Timestamp = {
    new Timestamp(sf.parse(dateStr).getTime)
  }
  
  //udfs
  object udfs extends Serializable {
    def toTimestamp = udf(parseDatetimeString _)
    val keywordParams = udf(() => "None")
    val translatePrediction = udf((prediction : Double) => if (prediction == 1) "positive" else "negative")
  }
  
  //text cleaning
  object textCleaningRegexes extends Serializable {
    val linkPattern = "((https?|ftp|gopher|telnet|file|Unsure|http):((//)|(\\\\))+[\\w\\d:#@%/;$()~_?\\+-=\\\\\\.&]*)"
    val emojiPattern = "[^\\p{L}\\p{M}\\p{N}\\p{P}\\p{Z}\\p{Cf}\\p{Cs}\\s]"
  }
  
}
