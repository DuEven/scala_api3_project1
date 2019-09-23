package util

/**
  * @author Duyw
  *         Date:2019/09/20  18:33:35
  * @Version ：1.0
  * @description:
  *
  */
object Self_MediaUtil {
  // 处理请求数
  def ReqPt(mediatype:Int):String={
    if(mediatype ==1){
      "长尾媒体"
    }else if(mediatype ==2){
      "视频媒体"
    }else if(mediatype ==3 ){
      "独立媒体"
    }else{
      "其他"
    }
  }

}
