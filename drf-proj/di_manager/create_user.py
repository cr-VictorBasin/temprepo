from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group, User
####
# Create groups
noc_group, created = Group.objects.get_or_create(name='noc_group')
dp_group, created = Group.objects.get_or_create(name='dp_group')
ds_group, created = Group.objects.get_or_create(name='ds_group')
####
UserModel = get_user_model()
####
usernames = ['noc_user', 'dp_user', 'ds_vico', 'ds_mati', 'ds_erez', 'ds_moshe', 'ds_postman']
####
for username in usernames:
    if not UserModel.objects.filter(username=username).exists():
        user = UserModel.objects.create_user(username, password='123456')
        if username.startswith('noc_') or username.startswith('dp_'):
            user.is_superuser = False
            user.is_staff = False
            # if username.startswith('noc_'):
            #username = User.objects.get(username=username)
            #noc_group.user_set.add(username)
            #if username.startswith('dp_'):
            #   dp_group.user_set.add(username)
        else:
            user.is_superuser = True
            user.is_staff = True
        user.save()
    if username.startswith('ds_'):
        ds_user = User.objects.get(username=username)
        ds_group.user_set.add(ds_user)

# # Get the user objects for the users you want to add to the groups
noc_user = User.objects.get(username='noc_user')
dp_user = User.objects.get(username='dp_user')

# # Add the users to their respective groups
noc_group.user_set.add(noc_user)
dp_group.user_set.add(dp_user)

