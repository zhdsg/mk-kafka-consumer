package com.zhimo.datahub.common

import java.net.{URL, URLDecoder}
import org.uaparser.scala.{Client, Parser}


object ParsingHelper {

  def decodeUrl(str:String): String ={
    try {
      URLDecoder.decode(if (str != null) str else "", "utf-8")
    }catch {
      case _:Throwable => str
    }
  }

  @transient private val parser:Parser = Parser.default

  def parseUA(string: String): MKUserAgent = {
    val netTypePattern = "[ \\t\\n\\r]*NetType\\/[ \\t]*([^ \\t\\n\\r]*)".r
    val netType = netTypePattern.findFirstIn(string).getOrElse("unknown").replace("NetType/","")
    val languagePattern = "[ \\t\\n\\r]*Language\\/[ \\t]*([^ \\t\\n\\r]*)".r
    val language = languagePattern.findFirstIn(string).getOrElse("unknown").replace("Language/","")

    MKUserAgent(parser.parse(string),language,netType)
  }

  def parseUrl(str:String):MKUrlWithContext = {
    val url = new URL(str)
    val path = decodeUrl(str.replace(url.getHost,"").replace(url.getProtocol+"://","").replace("/index.html?#",""))
    val pattern = "/([a-z][a-z,A-Z]*)/".r
    var pathForPattern = (if(path.endsWith("/")) path else path+"/").replace("/","//").replace("wechat","")
    val patternNameRemover = "name//([^/]*)/".r
    for(x<-patternNameRemover.findAllMatchIn(pathForPattern)){
      val name = x.group(0).replace("name//","").replace("/","")
      pathForPattern = pathForPattern.replace(name,"")
    }
    var withoutIds = ""
    if(pathForPattern.startsWith("/")){
      var skipNext = false
      for (x <- pattern.findAllMatchIn(pathForPattern)) {
        if(!skipNext) {
          val group = x.group(0).replace("/", "")
          withoutIds += group + "/"
          if(group=="talkcloud" || group == "name")skipNext = true
        }else{
          skipNext = false
        }
      }

    }
    if(path.startsWith("/wechat")){
      MKUrlWithContext(path.replace("/wechat",""),isWechat = true,withoutIds)
    }else{
      MKUrlWithContext(path,isWechat = false,withoutIds)
    }
  }

}

case class MKUrlWithContext(url:String,isWechat:Boolean,urlWithoutIds:String)

case class MKUserAgent(client:Client,language:String,connection:String) {
}