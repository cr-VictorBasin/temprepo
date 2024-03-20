from rest_framework import permissions
from rest_framework.permissions import BasePermission

class IsNocGroup(BasePermission):
    """
    Custom permission to only allow users who belong to the `noc_group`.
    """
    def has_permission(self, request, view):
        # Check if the user belongs to the `noc_group`
        return request.user.groups.filter(name='noc_group').exists()

# class AdminOrReadOnly(permissions.IsAdminUser):
#     def has_permission(self, request, view):
#         admin_permission = bool(request.user and request.user.is_staff)
#         return request == "POST" or admin_permission 

class IsAdminOrReadOnly(permissions.IsAdminUser):
    def has_permission(self, request, view):
        if request.method in permissions.SAFE_METHODS:
         # Check permissions for read-only request
            return True
        else:
            return  bool(request.user and request.user.is_staff)
    
        
class IsReviewUserOrReadOnly(permissions.BasePermission):
    def has_object_permission(self, request, view, obj):
        if request.method in permissions.SAFE_METHODS:
         # Check permissions for read-only request
            return True
        else:
            return obj.review_user == request.user
        