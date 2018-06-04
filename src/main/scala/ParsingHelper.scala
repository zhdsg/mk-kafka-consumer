import java.net.URLDecoder
import org.uaparser.scala.Parser
import org.uaparser.scala.Client

object ParsingHelper {

  def decodeUrl(str:String): String ={
    URLDecoder.decode(if(str!=null)str else "","utf-8")
  }

  def parseUA(string: String): MKUserAgent = {
    val netTypePattern = "[ \\t\\n\\r]*NetType\\/[ \\t]*([^ \\t\\n\\r]*)".r
    val netType = netTypePattern.findFirstIn(string).getOrElse("unknown").replace("NetType/","")
    val languagePattern = "[ \\t\\n\\r]*Language\\/[ \\t]*([^ \\t\\n\\r]*)".r
    val language = languagePattern.findFirstIn(string).getOrElse("unknown").replace("Language/","")

    MKUserAgent(Parser.default.parse(string),language,netType)
  }

}


case class MKUserAgent(client:Client,language:String,connection:String) {
}