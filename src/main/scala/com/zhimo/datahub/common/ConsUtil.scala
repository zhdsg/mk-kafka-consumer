package com.zhimo.datahub.common

/**
  * Created by yaning on 5/6/18.
  */
object ConsUtil {
  // mk's server log has a format of: YYYY-DD-MM HH:MM:SS.sss {json content}. Need to filter out the time part
  val MK_SERVER_LOG_ROW_OFFSET = 24
  val MK_SERVER_LOG_DATE_OFFSET = 10
  //  declarations for names of the server action logs
  val PAY_ACTION="pay"
  val REFUND_SUCCESS_ACTION="refund_success"
  val REFUND_VERIFICATION_FAILED_ACTION="refund_verify_failure"
  val REFUND_VERIFICATION_SUCCESS_ACTION="refund_verify_success"
  val REFUND_CANCELLED_ACTION="refund_cancel"
  val REFUND_APPLY_ACTION="refund_apply"

  // refund status
  val toBeVerify = 0
  val verifyFailed = 1
  val verifySucceeded = 2
  val cashRefunded = 3
  val canceled = 4
  val refundInProgress = 5
  val onlineRefunded = 6
  val workingInProgress = 7

  val toBeVerifyStr = "待审核"
  val verifyFailedStr = "审核失败"
  val verifySucceededStr = "审核成功"
  val cashRefundedStr = "退费成功(线下)"
  val canceledStr = "已取消"
  val refundInProgressStr = "退费中"
  val onlineRefundedStr = "退费成功(线上)"
  val workingInProgressStr = "平台处理中"

  val ALIPAY_IMMEDIATE = 2
  val ALIPAY_QRCODE = 43
  val WEIXIN_JS=4
  val WXM_PAY=30
  val UNION_POS=27
  val BILL_POS=28
  val POS=99
  val OFFLINE=1

  val ALIPAY_IMMEDIATE_STR = "支付宝"
  val ALIPAY_QRCODE_STR = "支付宝二维码"
  val WEIXIN_JS_STR="微信JS"
  val WXM_PAY_STR="微信二维码"
  val UNION_POS_STR="银联POS"
  val BILL_POS_STR="快钱POS"
  val POS_STR="POS"
  val OFFLINE_STR="现金"

}