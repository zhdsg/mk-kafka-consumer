/**
  * Created by yaning on 5/6/18.
  */
object ConsUtil {
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

}
