from opendc.models.user import User
from opendc.util import exceptions
from opendc.util.rest import Response


def GET(request):
    """Get this User."""

    try:
        request.check_required_parameters(path={'userId': 'string'})
    except exceptions.ParameterError as e:
        return Response(400, str(e))

    user = User.from_id(request.params_path['userId'])

    validation_error = user.validate()
    if validation_error is not None:
        return validation_error

    return Response(200, f'Successfully retrieved user.', user.obj)


def PUT(request):
    """Update this User's given name and/or family name."""

    try:
        request.check_required_parameters(body={'user': {
            'givenName': 'string',
            'familyName': 'string'
        }},
                                          path={'userId': 'string'})
    except exceptions.ParameterError as e:
        return Response(400, str(e))

    user = User.from_id(request.params_path['userId'])

    validation_error = user.validate(request.google_id)
    if validation_error is not None:
        return validation_error

    user.set_property('givenName', request.params_body['user']['givenName'])
    user.set_property('familyName', request.params_body['user']['familyName'])

    user.update()

    return Response(200, f'Successfully updated user.', user.obj)


def DELETE(request):
    """Delete this User."""

    try:
        request.check_required_parameters(path={'userId': 'string'})
    except exceptions.ParameterError as e:
        return Response(400, str(e))

    user = User.from_id(request.params_path['userId'])

    validation_error = user.validate(request.google_id)
    if validation_error is not None:
        return validation_error

    user.delete()

    return Response(200, f'Successfully deleted user.', user.obj)
