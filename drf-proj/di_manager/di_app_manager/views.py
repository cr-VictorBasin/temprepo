from rest_framework.decorators import APIView
from rest_framework.renderers import JSONRenderer, BrowsableAPIRenderer

from rest_framework.permissions import IsAuthenticated
from di_app_manager.permissions import IsNocGroup, IsAdminOrReadOnly, IsReviewUserOrReadOnly 

from di_app_manager.app_code.gcp_project import *


#from drf_yasg2.utils import swagger_auto_schema
from drf_spectacular.types import OpenApiTypes
from drf_spectacular.utils import extend_schema, OpenApiExample, OpenApiParameter, OpenApiResponse
from rest_framework import generics


from di_app_manager.serializers import *
import threading
import ast
from django.http import JsonResponse
      
def enable_logging():
    task_id = str(randrange(100000000, 999999999))
    log_file = '/code/logs/task_' + task_id + '.log'

    formatter = "%(asctime)-15s" \
                "| %(funcName)-11s"  \
                "| task_id: " + task_id + \
                "| %(levelname)-5s" \
                "| %(message)s"

    logging.config.dictConfig({
        'version': 1,
        'formatters': {
            'root_formatter': {
                'format': formatter
            }
        },
        'handlers': {
            'console': {
                'level': 'DEBUG',
                'class': 'logging.StreamHandler',
                'formatter': 'root_formatter'
            },
            'log_file': {
                'class': 'logging.FileHandler',
                'level': 'DEBUG',
                "encoding": "utf-8",
                'filename': log_file,
                'formatter': 'root_formatter',
            }
        },
        'loggers': {
            '': {
                'handlers': [
                    'console',
                    'log_file',
                ],
                'level': 'DEBUG',
                'propagate': True
            }
        }
    })
    logging.info('New task id allocated: ' + task_id)
    return task_id

    
class DeployNewGcpProject(APIView):
    permission_classes = [IsNocGroup]
    renderer_classes = (BrowsableAPIRenderer, JSONRenderer,)
    serializer_class = DeployNewGcpProjectSerializer

    @extend_schema(
        request=DeployNewGcpProjectSerializer,
        responses={200: ""},
        description="Create a new GCP project \n"
                    "Project name should be no more then 20 character \n"
                    "it will also open the following GCP API`s at the project level: \n"
                    " * compute.googleapis.com \n"
                    " * dns.googleapis.com \n"
                    " * monitoring.googleapis.com \n"
                    " * cloudresourcemanager.googleapis.com \n"
                    "\n"
                    "After project creation packer code will be executed automatically to create needed images.",
        operation_id='Deploy New GCP project'
    )
    def post(self, request):
        try:
            task_id = enable_logging()
            insert_task(task_id, 'DeployNewGcpProject', request, 'Running')
            t = threading.Thread(target=create_gcp_project, args=[int(task_id)],
                                 kwargs={'payload': request, 'action': 'deploy'}, daemon=True)
            t.start()
            return JsonResponse({"task_id": str(task_id), "status": "started"}, status=200)
        except Exception as error:
            #logging.error(str(error))
            return JsonResponse({"error": str(error)}, status=500)