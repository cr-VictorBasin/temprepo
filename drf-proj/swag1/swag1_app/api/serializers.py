from rest_framework import serializers
from swag1_app.models import Movie

class MovieSerializer(serializers.ModelSerializer):
    len_name = serializers.SerializerMethodField()
    
    class Meta:
        model = Movie
        fields = '__all__'
        #fields = ['id', 'name', 'description']
        #exclude = ['active']

    def get_len_name(self, object):
            return len(object.name)
        
    def validate(self, data):
            if data['name'] == data['description']:
                raise serializers.ValidationError('name and description cannot be the same.')
            return data
        
        
    def validate_name(self, value):
        if Movie.objects.filter(name=value).exists():
            raise serializers.ValidationError('Movie already exists')
        return value

def validate_description(self, value):
    if len(value) < 3:
        raise serializers.ValidationError('description too short.')
    return value
    

# class MovieSerializer(serializers.Serializer):
#     id = serializers.IntegerField(read_only=True)
#     name = serializers.CharField()
#     description = serializers.CharField()
#     active = serializers.BooleanField()
    
    # def create(self, validated_data):
    #     return Movie.objects.create(**validated_data)
    
    # def update(self, instance, validated_data):
    #     instance.name = validated_data.get('name', instance.name)
    #     instance.description = validated_data.get('description', instance.description)
    #     instance.active = validated_data.get('active', instance.active)
    #     instance.save()
    #     return instance
    
    # def validate(self, data):
    #     if data['name'] == data['description']:
    #         raise serializers.ValidationError('name and description cannot be the same.')
    #     return data
    
    
    # def validate_name(self, value):
    #     if Movie.objects.filter(name=value).exists():
    #         raise serializers.ValidationError('Movie already exists')
    #     return value
    
    # def validate_description(self, value):
    #     if len(value) < 3:
    #         raise serializers.ValidationError('description too short.')
    #     return value
    