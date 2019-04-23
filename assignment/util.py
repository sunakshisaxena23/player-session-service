import ndjson,json


try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

from functools import wraps
from flask import Response


def ndjson_api(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        result = f(*args, **kwargs)      # call function
        json_result = to_json(result)
        print(json_result)
        return Response(response=json_result,
                        status=200,
                        mimetype='application/x-ndjson')
    return decorated_function


def to_json(data):
    def handler(obj):
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        else:
            raise TypeError('Object of type %s with value of %s is not JSON serializable' % (type(obj), repr(obj)))

    return json.dumps(data, default=handler)


def json_api(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        result = f(*args, **kwargs)      # call function
        json_result = to_json(result)
        return Response(response=json_result,
                        status=200,
                        mimetype='application/json')
    return decorated_function


def _read_response(response):
    output = StringIO.StringIO()
    try:
        for line in response.response:
            output.write(line)

        return output.getvalue()

    finally:
        output.close()



