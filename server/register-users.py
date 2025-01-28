from flask import Flask, request, jsonify
from pymongo import MongoClient
from bson.objectid import ObjectId
import os
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

# Conectar ao MongoDB
# Configurar a conexão com o MongoDB
connection = "mongodb://127.0.0.1:27017/"  # Certifique-se de que o MongoDB está rodando localmente
client = MongoClient(connection)

# Especificar o banco de dados e a coleção
db = client['users']  # Nome do banco de dados
users_collection = db['users_db']  # Nome da coleção

# Criar um usuário
@app.route('/users', methods=['POST'])
def create_user():
    data = request.get_json()
    result = users_collection.insert_one(data)
    return jsonify({'id': str(result.inserted_id)}), 201

# Ler todos os usuários
@app.route('/users', methods=['GET'])
def get_users():
    users = list(users_collection.find())
    formatted_users = []
    for user in users:
        formatted_users.append({
            'id': str(user['_id']),  # Renomeia _id para id
            'name': user.get('name', ''),
            'email': user.get('email', ''),
            'age': user.get('age', 0)
        })
    return jsonify(formatted_users), 200

# Ler um usuário por ID
@app.route('/users/<id>', methods=['GET'])
def get_user(id):
    try:
        user = users_collection.find_one({'_id': ObjectId(id)})
        if user:
            user['_id'] = str(user['_id'])
            return jsonify(user), 200
        else:
            return jsonify({'error': 'User not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 400

# Atualizar um usuário
@app.route('/users/<id>', methods=['PUT'])
def update_user(id):
    data = request.get_json()
    try:
        result = users_collection.update_one({'_id': ObjectId(id)}, {'$set': data})
        if result.modified_count:
            return jsonify({'status': 'User updated'}), 200
        else:
            return jsonify({'error': 'User not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 400

# Deletar um usuário
@app.route('/users/<id>', methods=['DELETE'])
def delete_user(id):
    try:
        result = users_collection.delete_one({'_id': ObjectId(id)})
        if result.deleted_count:
            return jsonify({'status': 'User deleted'}), 200
        else:
            return jsonify({'error': 'User not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 400

if __name__ == '__main__':
    app.run(debug=True, port=5001)