from django.contrib.auth.models import User
from rest_framework.permissions import IsAdminUser
from rest_framework.views import APIView
from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework.authtoken.models import Token
from user_app.api.serializers import RegistrationSerializer
from rest_framework import status
from user_app  import models

@api_view(['POST',])
def logout_view(request):
    try:
         if request.method == 'POST':
             request.user.auth_token.delete()
             return Response(status=status.HTTP_200_OK)
    except Exception as AttributeError:
        return  Response(status=status.HTTP_401_UNAUTHORIZED)   
    
class DeleteAllUserTokens(APIView):
    """
    Deletes all existing user tokens.
    """
    permission_classes = (IsAdminUser,)

    def delete(self, request):
        try:
            # get all users
            users = User.objects.all()

            # delete all tokens for all users
            for user in users:
                tokens = Token.objects.filter(user=user)
                tokens.delete()

            return Response(status=status.HTTP_204_NO_CONTENT)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        
    
@api_view(['POST',])
def registration_view(request):
    
    if request.method == 'POST':
        serializer = RegistrationSerializer(data=request.data)
        
        data = {}
        
        if serializer.is_valid():
            account = serializer.save()
            
            data['response'] = "Registration successful"  
            data['username'] = account.username
            data['email'] = account.email    
            
            token = Token.objects.get(user=account).key
            data['token'] = token   
            
        else:
            data = serializer.errors
               
        return  Response(data)