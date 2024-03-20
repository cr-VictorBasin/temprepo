from rest_framework import serializers
#from di_app_manager.models import Movie
# from drf_yasg2 import OpenApiTypes
from drf_spectacular.utils import extend_schema, OpenApiParameter, OpenApiTypes
#from drf_spectacular.types import OpenApiTypes

class DeployNewGcpProjectSerializer(serializers.Serializer):
    project_name = serializers.CharField(
        max_length=20,
        help_text="The GCP project name",
        label="Project Name",
       # required=True,
        default="vico-project22",
    )
    env_type = serializers.ChoiceField(
        choices=["prod", "dr", "dev", "csdev"],
        help_text="target GCP project env type, ie prod, dr, dev or csdev",
        label="Environment Type",
        required=True
    )
    certificates_fqdn = serializers.CharField(
        help_text="target fqdn the certificates will be created for",
        label="Certificates FQDN",
        required=True
    )
    target_mail_address = serializers.EmailField(
        help_text="target email address to notify about this api call status",
        label="Target Email Address",
        #required=True,
        default='vico.basin@cybereason.com'
    )
      
    class Meta:
        # description = "Create a new GCP project \n" \
        #             "Project name should be no more then 20 character \n" \
        #             "it will also open the following GCP API`s at the project level: \n" \
        #             " * compute.googleapis.com \n" \
        #             " * dns.googleapis.com \n" \
        #             " * monitoring.googleapis.com \n" \
        #             " * cloudresourcemanager.googleapis.com \n" \
        #             "\n" \
        #             "After project creation packer code will be executed automatically to create needed images."
        ref_name = 'DeployNewGcpProjectSerializer'
        extend_schema = {
            "type": OpenApiTypes.OBJECT,
            "properties": {
                "project_name": {
                    "title": "text",
                    "description": "The GCP project name",
                    "type": OpenApiTypes.STR,
                    "required":True,
                },
                "env_type": {
                    "title": "text",
                    "description": "target GCP project env type, ie prod, dr, dev or csdev",
                    "type": OpenApiTypes.STR,
                    "example": "prod, dr, dev or csdev",
                },
                "certificates_fqdn": {
                    "title": "text",
                    "description": "target fqdn the certificates will be created for",
                    "type": OpenApiTypes.STR,
                    "example": "eng.cybereason.net or prod.cybereason.net for DI",
                },
                "target_mail_address": {
                    "title": "text",
                    "description": "target email address to notify about this api call status",
                    "type": OpenApiTypes.STR,
                    "required":True,
                }
            }
        }

