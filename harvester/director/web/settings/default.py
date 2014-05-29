# ======================================================================
# Base settings for harvester director
# ======================================================================

# ----------------------------------------------------------------------
# Flask settings
# ----------------------------------------------------------------------

# enable/disable debug mode
# DEBUG = None

# enable/disable testing mode
# TESTING = None

# explicitly enable or disable the propagation of exceptions. If not
# set or explicitly set to None this is implicitly true if either
# TESTING or DEBUG is true.
# PROPAGATE_EXCEPTIONS = None

# By default if the application is in debug mode the request context
# is not popped on exceptions to enable debuggers to introspect the
# data. This can be disabled by this key. You can also use this
# setting to force-enable it for non debug execution which might be
# useful to debug production applications (but also very risky).
# PRESERVE_CONTEXT_ON_EXCEPTION = None

# the secret key
# SECRET_KEY = None

# the name of the session cookie
# SESSION_COOKIE_NAME = None

# the domain for the session cookie. If this is not set, the cookie
# will be valid for all subdomains of SERVER_NAME.
# SESSION_COOKIE_DOMAIN = None

# the path for the session cookie. If this is not set the cookie
# will be valid for all of APPLICATION_ROOT or if that is not set
# for '/'.
# SESSION_COOKIE_PATH = None

# controls if the cookie should be set with the httponly
# flag. Defaults to True.
# SESSION_COOKIE_HTTPONLY = None

# controls if the cookie should be set with the secure
# flag. Defaults to False.
# SESSION_COOKIE_SECURE = None

# the lifetime of a permanent session as datetime.timedelta
# object. Starting with Flask 0.8 this can also be an integer
# representing seconds.
# PERMANENT_SESSION_LIFETIME = None

# this flag controls how permanent sessions are refreshed. If set to
# True (which is the default) then the cookie is refreshed each
# request which automatically bumps the lifetime. If set to False a
# set-cookie header is only sent if the session is modified. Non
# permanent sessions are not affected by this.
# SESSION_REFRESH_EACH_REQUEST = None

# enable/disable x-sendfile
# USE_X_SENDFILE = None

# the name of the logger
# LOGGER_NAME = None

# the name and port number of the server. Required for subdomain
# support (e.g.: 'myapp.dev:5000') Note that localhost does not
# support subdomains so setting this to "localhost" does not
# help. Setting a SERVER_NAME also by default enables URL generation
# without a request context but with an application context.
# SERVER_NAME = 'localhost:5000'

# If the application does not occupy a whole domain or subdomain
# this can be set to the path where the application is configured to
# live. This is for session cookie as path value. If domains are
# used, this should be None.
# APPLICATION_ROOT = None

# If set to a value in bytes, Flask will reject incoming requests
# with a content length greater than this by returning a 413 status
# code.
# MAX_CONTENT_LENGTH = None

# :  Default cache control max age to use with send_static_file() (the
# default static file handler) and send_file(), in seconds. Override
# this value on a per-file basis using the get_send_file_max_age()
# hook on Flask or Blueprint, respectively. Defaults to 43200 (12
# hours).
# SEND_FILE_MAX_AGE_DEFAULT = None

# If this is set to True Flask will not execute the error handlers
# of HTTP exceptions but instead treat the exception like any other
# and bubble it through the exception stack. This is helpful for
# hairy debugging situations where you have to find out where an
# HTTP exception is coming from.
# TRAP_HTTP_EXCEPTIONS = None

# Werkzeug's internal data structures that deal with request
# specific data will raise special key errors that are also bad
# request exceptions. Likewise many operations can implicitly fail
# with a BadRequest exception for consistency. Since it's nice for
# debugging to know why exactly it failed this flag can be used to
# debug those situations. If this config is set to True you will get
# a regular traceback instead.
# TRAP_BAD_REQUEST_ERRORS = None

# The URL scheme that should be used for URL generation if no URL
# scheme is available. This defaults to http.
# PREFERRED_URL_SCHEME = None

# By default Flask serialize object to ascii-encoded JSON. If this
# is set to False Flask will not encode to ASCII and output strings
# as-is and return unicode strings. jsonify will automatically
# encode it in utf-8 then for transport for instance.
# JSON_AS_ASCII = None

# By default Flask will serialize JSON objects in a way that the
# keys are ordered. This is done in order to ensure that independent
# of the hash seed of the dictionary the return value will be
# consistent to not trash external HTTP caches. You can override the
# default behavior by changing this variable. This is not
# recommended but might give you a performance improvement on the
# cost of cachability.
# JSON_SORT_KEYS = None

# If this is set to True (the default) jsonify responses will be
# pretty printed if they are not requested by an XMLHttpRequest
# object (controlled by the X-Requested-With header)
# JSONIFY_PRETTYPRINT_REGULAR = None

# ----------------------------------------------------------------------
# Celery settings
# ----------------------------------------------------------------------

CELERY_BROKER_URL = 'redis://localhost:6379'
CELERY_RESULT_BACKEND = 'redis://localhost:6379'

# Pickle is dangerous, but let's accept other formats.
CELERY_ACCEPT_CONTENT = ['json', 'msgpack', 'yaml']

# Use json by default to serialize messages, instead of pickle.
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'

# ----------------------------------------------------------------------
# Harvester director settings
# ----------------------------------------------------------------------

HARVESTER_MONGODB = {
    'host': 'mongodb://localhost:27017',
    'name': 'harvester',
}
