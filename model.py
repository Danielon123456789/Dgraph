# Establish Dgraph client
import csv
import json
import pydgraph
import datetime


client_stub = pydgraph.DgraphClientStub('localhost:9080')
client = pydgraph.DgraphClient(client_stub)

def set_schema(client):
    schema = """
    
    type User {
        username
        email
        phone
        birthdate
        created_at
        ha_comprado
        tiene_favoritos
        hizo_devolucion
    }

    type Producto {
        nombre
        precio
        descripcion
        stock
        tiene_categoria
    }
    
    type Categoria {
        categoria
    }
    
    type devolucion {
        motivo
        De_producto
    }

    username: string @index(exact) .
    email: string .
    phone: string .
    birthdate: datetime .
    created_at: datetime .
    ha_comprado: [uid] .
    tiene_favoritos: [uid] .
    hizo_devolucion: [uid] .
    
    nombre: string @index(fulltext) .
    precio: float @index(float) . 
    descripcion: string .
    stock: int .
    tiene_categoria: [uid] @reverse .
    
    categoria: string @index(exact) .
    
    motivo: string .
    De_producto: [uid] .
    
    """
    return client.alter(pydgraph.Operation(schema=schema))


#Data:

def load_Users(file_path):
    txn = client.txn()
    resp = None
    try:
        Users = []
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                Users.append({
                    'uid': '_:' + row['user_id'],  # Añadir _: para que Dgraph lo reconozca como un identificador personalizado
                    'dgraph.type': 'User',
                    'username': row['username'],
                    'email': row['email'],
                    'phone': row['phone'],  
                    'birthdate': row['birthdate'],  
                    'created_at': row['created_at']
                })
            print(f"Loading users: {Users}")
            resp = txn.mutate(set_obj=Users)
        txn.commit()
    finally:
        txn.discard()
    return resp.uids


def load_productos(file_path):
    txn = client.txn()
    resp = None
    try:
        Producto = []
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                Producto.append({
                    'uid': '_:' + row['Productos_id'],  # Añadir _: para los identificadores personalizados
                    'dgraph.type': 'Producto',
                    'nombre': row['nombre'],
                    'precio': float(row['precio']),
                    'descripcion': row['descripcion'],
                    'stock': int(row['stock'])
                })
            print(f"Loading Productos: {Producto}")
            resp = txn.mutate(set_obj=Producto)
        txn.commit()
    finally:
        txn.discard()
    return resp.uids


def load_devolucion(file_path):
    txn = client.txn()
    resp = None
    try:
        devoluciones = []
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                devoluciones.append({
                    'uid': '_:' + row['devolucion_id'],  # Añadir _: para los identificadores personalizados
                    'dgraph.type': 'devolucion',
                    'motivo': row['motivo'],
                })
            print(f"Loading devoluciones: {devoluciones}")
            resp = txn.mutate(set_obj=devoluciones)
        txn.commit()
    finally:
        txn.discard()
    return resp.uids

def load_categoria(file_path):
    txn = client.txn()
    resp = None
    try:
        categoria = []
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                categoria.append({
                    'uid': '_:' + row['categoria'],  # Añadir _: para los identificadores personalizados
                    'dgraph.type': 'Categoria',
                    'categoria': row['categoria']
                })
            print(f"Loading categorias: {categoria}")
            resp = txn.mutate(set_obj=categoria)
        txn.commit()
    finally:
        txn.discard()
    return resp.uids


#Relaciones:

def ha_comprado(file_path, users_uids, productos_uids):
    from collections import defaultdict
    txn = client.txn()
    try:
        relaciones = defaultdict(list)
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                username = row['user_id']
                producto_id = row['Productos_id']
                if username in users_uids and producto_id in productos_uids:
                    relaciones[users_uids[username]].append({'uid': productos_uids[producto_id]})
        
        for user_uid, productos in relaciones.items():
            mutation = {
                'uid': user_uid,
                'ha_comprado': productos
            }
            print(f"Relacionando usuario {user_uid} con productos comprados: {[p['uid'] for p in productos]}")
            txn.mutate(set_obj=mutation)
        
        txn.commit()
    finally:
        txn.discard()


def hizo_devolucion(file_path, users_uids, devoluciones_uids):
    from collections import defaultdict
    txn = client.txn()
    try:
        relaciones = defaultdict(list)
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                user = row['user_id']
                devolucion_id = row['devolucion_id']
                if user in users_uids and devolucion_id in devoluciones_uids:
                    relaciones[users_uids[user]].append({'uid': devoluciones_uids[devolucion_id]})
        
        for user_uid, devoluciones in relaciones.items():
            mutation = {
                'uid': user_uid,
                'hizo_devolucion': devoluciones
            }
            print(f"Relacionando usuario {user_uid} con devoluciones: {[d['uid'] for d in devoluciones]}")
            txn.mutate(set_obj=mutation)
        
        txn.commit()
    finally:
        txn.discard()

def producto_categoria(file_path, productos_uids, categorias_uids):
    txn = client.txn()
    try:
        relaciones = []
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                producto_id = row['producto_id']
                categoria = row['categoria']
                if producto_id in productos_uids and categoria in categorias_uids:
                    relaciones.append({
                        'uid': productos_uids[producto_id],
                        'tiene_categoria': [{'uid': categorias_uids[categoria]}]
                    })

        for relacion in relaciones:
            print(f"Relacionando producto {relacion['uid']} con categoría.")
            txn.mutate(set_obj=relacion)

        txn.commit()
    finally:
        txn.discard()

def tiene_favoritos(file_path, users_uids, productos_uids):
    from collections import defaultdict
    txn = client.txn()
    try:
        relaciones = defaultdict(list)
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                user = row['user_id']
                prod = row['Productos_id']
                if user in users_uids and prod in productos_uids:
                    relaciones[users_uids[user]].append({'uid': productos_uids[prod]})

        for user_uid, productos in relaciones.items():
            mutation = {
                'uid': user_uid,
                'tiene_favoritos': productos
            }
            print(f"Relacionando {user_uid} con favoritos: {[p['uid'] for p in productos]}")
            txn.mutate(set_obj=mutation)

        txn.commit()
    finally:
        txn.discard()



def de_producto(file_path, devoluciones_uids, productos_uids):
    txn = client.txn()
    try:
        relaciones = []
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                devolucion_id = row['devolucion_id']
                producto_id = row['producto_id']
                if devolucion_id in devoluciones_uids and producto_id in productos_uids:
                    relaciones.append({
                        'uid': devoluciones_uids[devolucion_id],
                        'De_producto': [{'uid': productos_uids[producto_id]}]
                    })

        for relacion in relaciones:
            print(f"Relacionando devolución {relacion['uid']} con producto devuelto.")
            txn.mutate(set_obj=relacion)

        txn.commit()
    finally:
        txn.discard()


def tiene_categoria(file_path, productos_uids, categorias_uids):
    txn = client.txn()
    try:
        relaciones = []
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                producto_id = row['Productos_id']
                categoria = row['categoria']
                if producto_id in productos_uids and categoria in categorias_uids:
                    relaciones.append({
                        'uid': productos_uids[producto_id],
                        'tiene_categoria': [{'uid': categorias_uids[categoria]}]
                    })

        for relacion in relaciones:
            print(f"Relacionando producto {relacion['uid']} con categoría {relacion['tiene_categoria'][0]['uid']}.")
            txn.mutate(set_obj=relacion)

        txn.commit()
    finally:
        txn.discard()



def create_data(client):
    # Cargar usuarios
    users_uids = load_Users('User.csv')
    print(f"Usuarios cargados: {users_uids}")

    # Cargar productos
    productos_uids = load_productos('productos.csv')
    print(f"Productos cargados: {productos_uids}")

    # Cargar categorías
    categorias_uids = load_categoria('categorias.csv')
    print(f"Categorías cargadas: {categorias_uids}")

    # Cargar devoluciones
    devoluciones_uids = load_devolucion('devoluciones.csv')
    print(f"Devoluciones cargadas: {devoluciones_uids}")

    # Crear relaciones
    tiene_favoritos('favoritos.csv', users_uids, productos_uids)
    ha_comprado('ha_comprado.csv', users_uids, productos_uids)
    hizo_devolucion('hizo_devolucion.csv', users_uids, devoluciones_uids)
    tiene_categoria('producto_categoria.csv', productos_uids, categorias_uids)
    de_producto('de_productos.csv', devoluciones_uids, productos_uids)

    print("Todos los datos y relaciones fueron creados correctamente.")


#Buscar usuario:

def search_users(client, username):
    query = f"""
    {{
        search_user(func: eq(username, "{username}")) @filter(type(User)) {{
            username
            email
            phone
            birthdate
            created_at
            ha_comprado{{
                nombre
                precio
            }} 
            tiene_favoritos{{
                nombre
                precio
            }}
            hizo_devolucion {{
                motivo
                De_producto {{
                    nombre 
                    precio
                }}
            }}
            
        }}
    }}
    """
    txn = client.txn(read_only=True)
    try:
        res = txn.query(query)
        data = json.loads(res.json)
        usuarios = data.get("search_user", [])
        
        if not usuarios:
            print(f"Usuario '{username}' no encontrado.")
            return None

        usuario = usuarios[0]
        print(f"Usuario encontrado:\n{json.dumps(usuario, indent=2)}")
        return usuario
    finally:
        txn.discard()


# Queries guardar en favoritos:
def guardar_en_favoritos(client, username, productoNombre):
    # 1. Buscar el UID del producto por nombre
    query_producto = f"""
    {{
        search_producto(func: eq(nombre, "{productoNombre}")) @filter(type(Producto)) {{
            uid
            nombre
        }}
    }}
    """
    txn = client.txn(read_only=True)
    try:
        res = txn.query(query_producto)
        data = json.loads(res.json)
        productos = data.get("search_producto", [])
        
        if not productos:
            print(f"Producto '{productoNombre}' no encontrado.")
            return
        producto_uid = productos[0]["uid"]
        print(f"Producto encontrado: {productoNombre} -> UID: {producto_uid}")
    finally:
        txn.discard()

    # 2. Buscar el UID del usuario por username
    query_user = f"""
    {{
        search_user(func: eq(username, "{username}")) @filter(type(User)) {{
            uid
            username
        }}
    }}
    """
    txn = client.txn(read_only=True)
    try:
        res = txn.query(query_user)
        data = json.loads(res.json)
        usuarios = data.get("search_user", [])
        
        if not usuarios:
            print(f"Usuario '{username}' no encontrado.")
            return
        user_uid = usuarios[0]["uid"]
        print(f"Usuario encontrado: {username} -> UID: {user_uid}")
    finally:
        txn.discard()

    # 3. Agregar el producto a favoritos del usuario
    txn = client.txn()
    try:
        mutation = {
            'uid': user_uid,
            'tiene_favoritos': [{'uid': producto_uid}]
        }
        txn.mutate(set_obj=mutation, commit_now=True)
        print(f"Producto '{productoNombre}' agregado a favoritos del usuario '{username}'.")
    finally:
        txn.discard()


# Query para hacer la devolucion de un producto.
def registrar_devolucion(client, nombreProducto, motivo, username):
    # 1. Buscar UID del producto
    query_producto = f"""
    {{
        producto(func: eq(nombre, "{nombreProducto}")) @filter(type(Producto)) {{
            uid
            nombre
        }}
    }}
    """
    txn = client.txn(read_only=True)
    try:
        res = txn.query(query_producto)
        data = json.loads(res.json)
        productos = data.get("producto", [])
        if not productos:
            print(f"Producto '{nombreProducto}' no encontrado.")
            return
        producto_uid = productos[0]["uid"]
    finally:
        txn.discard()

    # 2. Buscar UID del usuario
    query_user = f"""
    {{
        user(func: eq(username, "{username}")) @filter(type(User)) {{
            uid
            username
        }}
    }}
    """
    txn = client.txn(read_only=True)
    try:
        res = txn.query(query_user)
        data = json.loads(res.json)
        usuarios = data.get("user", [])
        if not usuarios:
            print(f"Usuario '{username}' no encontrado.")
            return
        user_uid = usuarios[0]["uid"]
    finally:
        txn.discard()

    # 3. Crear el nodo de devolución
    txn = client.txn()
    try:
        devolucion_node = {
            'uid': '_:devolucion',
            'dgraph.type': 'devolucion',
            'motivo': motivo
        }
        res = txn.mutate(set_obj=devolucion_node, commit_now=True)
        devolucion_uid = res.uids.get('devolucion')
        if not devolucion_uid:
            print("Error al crear la devolución.")
            return
        print(f"Devolución creada con UID: {devolucion_uid}")
    finally:
        txn.discard()

    # 4. Relacionar usuario con devolución
    txn = client.txn()
    try:
        relacion1 = {
            'uid': user_uid,
            'hizo_devolucion': [{'uid': devolucion_uid}]
        }
        txn.mutate(set_obj=relacion1, commit_now=True)
        print(f"Relacionada devolución con usuario {username}.")
    finally:
        txn.discard()

    # 5. Relacionar devolución con producto
    txn = client.txn()
    try:
        relacion2 = {
            'uid': devolucion_uid,
            'De_producto': [{'uid': producto_uid}]
        }
        txn.mutate(set_obj=relacion2, commit_now=True)
        print(f"Relacionada devolución con producto '{nombreProducto}'.")
    finally:
        txn.discard()


# Query historial de devolucion 

def devoluciones_por_usuario(client, username):
    query = f"""
    {{
        devoluciones_usuario(func: eq(username, "{username}")) @filter(type(User)) {{
            username
            hizo_devolucion @filter(type(devolucion)) {{
                motivo
                De_producto @filter(type(Producto)) {{
                    nombre
                    precio
                }}
            }}
        }}
    }}
    """
    txn = client.txn(read_only=True)
    try:
        res = txn.query(query)
        data = json.loads(res.json)
        devoluciones = data.get("devoluciones_usuario", [])

        if not devoluciones:
            print(f"No se encontraron devoluciones para el usuario '{username}'.")
            return

        print(f"Devoluciones del usuario '{username}':\n{json.dumps(devoluciones, indent=2)}")
    finally:
        txn.discard()

        
# obtener los favoritos del usuario.
def favoritos_del_usuario(client, username):
    query = f"""
    {{
        usuario_favoritos(func: eq(username, "{username}")) @filter(type(User)) {{
            username
            tiene_favoritos @filter(type(Producto)) {{
                nombre
                precio
                descripcion
                stock
            }}
        }}
    }}
    """
    txn = client.txn(read_only=True)
    try:
        res = txn.query(query)
        data = json.loads(res.json)
        favoritos = data.get("usuario_favoritos", [])

        if not favoritos:
            print(f"No se encontraron productos favoritos para el usuario '{username}'.")
            return

        print(f"Favoritos del usuario '{username}':\n{json.dumps(favoritos, indent=2)}")
    finally:
        txn.discard()

# Recomendacion de productos.
def recomendaciones_por_categoria(client, username):
    query = f"""
    {{
        var(func: eq(username, "{username}")) @filter(type(User)) {{
            fav as tiene_favoritos @filter(type(Producto)) {{
                cat as tiene_categoria
            }}
        }}

        recomendaciones(func: uid(cat)) @filter(type(Categoria)) {{
            categoria
            recomendaciones: ~tiene_categoria @filter(type(Producto) AND NOT uid(fav)) (first: 2) {{
                uid
                nombre
                precio
                descripcion
                stock
            }}
        }}
    }}
    """
    txn = client.txn(read_only=True)
    try:
        res = txn.query(query)
        data = json.loads(res.json)
        print(f"Recomendaciones para usuario '{username}':\n{json.dumps(data, indent=2)}")
    finally:
        txn.discard()

    
def drop_all(client):
    return client.alter(pydgraph.Operation(drop_all=True))
