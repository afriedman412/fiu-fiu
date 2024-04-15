from flask import Blueprint, abort, jsonify, request
from .helpers import send_email, EMAIL_FROM
from .logger import logger


error_routes = Blueprint("error_rotes", __name__)


@error_routes.errorhandler(500)
def internal_server_error(error):
    """
    Emails me when a 500 error happens, plus some detailed logging for reference.
    """
    endpoint = request.path
    payload = request.json if request.is_json else None

    error_message = {
        "error": "Internal Server Error",
        "message": error.description,
        "endpoint": endpoint,
        "payload": payload
    }
    response = jsonify(error_message)
    response.status_code = 500
    logger.error(error.description)
    body = "\n".join([f"{k}: {v}" for k, v in error_message.items()])
    logger.error(body)
    send_email(
        "Internal Server Error on fiufiu",
        body,
        from_email=EMAIL_FROM,
        to_email=EMAIL_FROM
        )
    return response


@error_routes.route("/test500")
def internal_server_error_test():
    """
    Test route for 500 error alerts.
    """
    logger.debug("ISE test...")
    try:
        raise Exception("look its an internal server error")
    except Exception as e:
        abort(500, description="Internal server error: " + str(e))
