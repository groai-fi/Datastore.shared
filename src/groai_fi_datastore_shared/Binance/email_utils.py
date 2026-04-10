"""Email utilities (optional dependency)"""
from typing import Optional, Callable, List


class EmailSender:
    """Email sender with callback pattern"""
    
    def __init__(self, callback: Optional[Callable] = None):
        """
        Initialize email sender
        
        Args:
            callback: Optional function(receivers: List[str], subject: str, content: str) -> None
        """
        self.callback = callback
    
    def send_mail(self, receivers: List[str], subject: str, content: str):
        """Send email via callback or no-op
        
        Args:
            receivers: List of email addresses
            subject: Email subject
            content: Email content
        """
        if self.callback:
            try:
                self.callback(receivers, subject, content)
            except Exception as e:
                print(f"Failed to send email: {e}")
        else:
            # No-op if no callback provided - just log
            print(f"[Email] To: {', '.join(receivers)}")
            print(f"[Email] Subject: {subject}")
            print(f"[Email] Content: {content[:100]}...")


# Backward compatibility function
def send_mail(receivers: List[str], subject: str, content: str):
    """Send email (backward compatibility wrapper)
    
    This function provides backward compatibility for code that imports:
    from api.SendMail.SendMail import send_mail
    """
    sender = EmailSender()
    sender.send_mail(receivers, subject, content)
