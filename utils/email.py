"""Email alerting utilities extracted from ae_server.py."""
# NOTE: generate_daily_report and send_daily_report reference globals from
# state.py — will be wired after state.py is created

import os
import logging
import smtplib
from datetime import datetime, timezone
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# ==================== 邮件报警配置 ====================
ALERT_EMAIL = "13910306825@163.com"  # 报警接收邮箱


def send_email_alert(subject: str, message: str):
    """发送邮件报警"""
    try:
        # 使用163邮箱SMTP服务（免费，需要授权码）
        # 注意：需要在环境变量中配置邮箱和授权码
        sender_email = os.getenv("SMTP_EMAIL")  # 发件邮箱
        sender_password = os.getenv("SMTP_PASSWORD")  # 授权码（不是邮箱密码）

        if not sender_email or not sender_password:
            logging.warning("⚠️ 未配置邮件发送账号，跳过邮件报警")
            return

        # 创建邮件
        msg = MIMEMultipart()
        msg["From"] = sender_email
        msg["To"] = ALERT_EMAIL
        msg["Subject"] = f"[AE交易系统] {subject}"

        # 邮件正文
        body = f"""
AE自动交易系统报警

时间: {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")}

{message}

---
此邮件由AE交易系统自动发送
服务器: {os.uname().nodename if hasattr(os, "uname") else "Unknown"}
"""
        msg.attach(MIMEText(body, "plain", "utf-8"))

        # 发送邮件
        with smtplib.SMTP_SSL("smtp.163.com", 465, timeout=10) as server:
            server.login(sender_email, sender_password)
            server.send_message(msg)

        logging.info(f"✅ 邮件报警已发送: {subject}")

    except Exception as e:
        logging.error(f"❌ 发送邮件报警失败: {e}")
