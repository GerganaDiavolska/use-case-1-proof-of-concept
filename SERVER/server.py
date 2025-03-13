from flask import Flask, jsonify, request, abort
from snowflake.snowpark import Session


SNOWFLAKE_CONFIG = {
    "user": "GERGANADIAVOLSKA",
    "password": "SugaJhopeRm777",
    "account": "ATLSZRI-IV85483",
    "warehouse": "COMPUTE_WH",
    "database": "LUNAPARKMADHOUSE",
    "schema": "PUBLIC",
    "role": "ACCOUNTADMIN"
}


def get_snowflake_session():
    return Session.builder.configs(SNOWFLAKE_CONFIG).create()


app = Flask(__name__)


@app.route('/')
def index():
    return "Welcome to the Amusement Park API!"


def fetch_results(query, params=None):
    session = get_snowflake_session()
    df = session.sql(query).collect() if not params else session.sql(query).bind(params).collect()
    session.close()
    return [row.as_dict() for row in df]


@app.route('/attractions', methods=['GET'])
def get_attractions():
    return jsonify(fetch_results("SELECT * FROM ATTRACTION"))


@app.route('/attractions/<int:id>', methods=['GET'])
def get_attraction(id):
    result = fetch_results(f"SELECT * FROM ATTRACTION WHERE ATTRACTIONID = {id}")
    return jsonify(result[0]) if result else abort(404)


@app.route('/employees', methods=['GET'])
def get_employees():
    return jsonify(fetch_results("SELECT * FROM EMPLOYEE"))


@app.route('/employees/<int:id>', methods=['GET'])
def get_employee(id):
    result = fetch_results(f"SELECT * FROM EMPLOYEE WHERE EMPLOYEEID = {id}")
    return jsonify(result[0]) if result else abort(404)


@app.route('/tickets', methods=['GET'])
def get_tickets():
    return jsonify(fetch_results("SELECT * FROM TICKET"))


@app.route('/tickets/<int:id>', methods=['GET'])
def get_ticket(id):
    result = fetch_results(f"SELECT * FROM TICKET WHERE TICKETID = {id}")
    return jsonify(result[0]) if result else abort(404)


@app.route('/events', methods=['GET'])
def get_events():
    return jsonify(fetch_results("SELECT * FROM EVENT"))


@app.route('/events/<int:id>', methods=['GET'])
def get_event(id):
    result = fetch_results(f"SELECT * FROM EVENT WHERE EVENTID = {id}")
    return jsonify(result[0]) if result else abort(404)


@app.route('/visitors', methods=['GET'])
def get_visitors():
    return jsonify(fetch_results("SELECT * FROM VISITOR"))


@app.route('/visitors/<int:id>', methods=['GET'])
def get_visitor(id):
    result = fetch_results(f"SELECT * FROM VISITOR WHERE VISITORID = {id}")
    return jsonify(result[0]) if result else abort(404)


@app.route('/financial', methods=['GET'])
def get_financial():
    return jsonify(fetch_results("SELECT * FROM FINANCIAL_TRANSACTION"))


@app.route('/financial/<int:id>', methods=['GET'])
def get_financial_transaction(id):
    result = fetch_results(f"SELECT * FROM FINANCIAL_TRANSACTION WHERE FINANCIALID = {id}")
    return jsonify(result[0]) if result else abort(404)


@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Not found"}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({"error": "Internal server error"}), 500


if __name__ == "__main__":
    app.run(debug=True)


