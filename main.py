#!/usr/bin/env python3
import os
import pydgraph
import model

DGRAPH_URI = os.getenv('DGRAPH_URI', 'localhost:9080')

def print_menu():
    mm_options = {
        1: "Create data",
        2: "Search user",
        3: "favoritos del usuario",
        4: "devoluciones del usuario",
        5: "guardar en favoritos",
        6: "guardar devolucion",
        7: "recomendacion por categoria",
        8: "Drop All",
        9: "Exit",
    }
    for key in mm_options.keys():
        print(key, '--', mm_options[key])


# Conexión directa con el servidor Dgraph usando gRPC.
def create_client_stub():
    return pydgraph.DgraphClientStub(DGRAPH_URI)

# Crea el cliente principal de Dgraph: realizar operaciones de alto nivel como queries, mutaciones y transacciones.
def create_client(client_stub):
    return pydgraph.DgraphClient(client_stub)

def close_client_stub(client_stub):
    client_stub.close()

def main():
    # Inicializar Client Stub y Dgraph Client
    client_stub = create_client_stub()
    client = create_client(client_stub)

    # Crear schema
    model.set_schema(client)

    while(True):
        print_menu()
        option = int(input('Enter your choice: '))

        if option == 1:
            model.create_data(client)

        elif option == 2:
            username = input("Username: ")
            model.search_users(client, username)

        elif option == 3:
            username = input("Username: ")
            model.favoritos_del_usuario(client, username)

        elif option == 4:
            username = input("Username: ")
            model.devoluciones_por_usuario(client, username)

        elif option == 5:
            username = input("Username: ")
            producto = input("Nombre del producto: ")
            model.guardar_en_favoritos(client, username, producto)

        elif option == 6:
            username = input("Username: ")
            producto = input("Nombre del producto a devolver: ")
            motivo = input("Motivo de la devolución: ")
            model.registrar_devolucion(client, producto, motivo, username)

        elif option == 7:
            username = input("Username: ")
            model.recomendaciones_por_categoria(client, username)

        elif option == 8:
            model.drop_all(client)

        elif option == 9:
            model.drop_all(client)
            close_client_stub(client_stub)
            print("Sesión finalizada.")
            exit(0)

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print('Error: {}'.format(e))