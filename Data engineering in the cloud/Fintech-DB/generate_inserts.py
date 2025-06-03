import random
from datetime import datetime, timedelta

# Helper functions
def random_date(start, end):
    return start + timedelta(days=random.randint(0, (end - start).days))

nombres_m = ['Juan','Carlos','Luis','Pedro','David','Andrés','Felipe','Santiago','José','Miguel','Daniel','Roberto','Jorge','Alberto','Francisco']
nombres_f = ['María','Ana','Laura','Sofía','Camila','Valentina','Isabella','Daniela','Carolina','Patricia','Claudia','Luisa','Adriana','Mariana','Juliana']
apellidos = ['Gómez','Rodríguez','López','Martínez','García','Pérez','Sánchez','Ramírez','Torres','Díaz','González','Hernández','Morales','Rojas','Silva']
ciudades = ['Bogotá','Medellín','Cali','Barranquilla','Cartagena']
estado_civil_opts = ['Soltero', 'Casado', 'Divorciado', 'Viudo']
educacion_opts = ['Bachiller', 'Técnico', 'Universitario', 'Posgrado']
genero_opts = ['Masculino', 'Femenino']

clientes = []
for i in range(1, 101):
    cliente_id = f"CL{str(i).zfill(3)}"
    genero = random.choice(genero_opts)
    nombre = random.choice(nombres_m if genero == 'Masculino' else nombres_f)
    apellido = random.choice(apellidos)
    edad = random.randint(18, 68)
    estado_civil = random.choice(estado_civil_opts)
    educacion = random.choice(educacion_opts)
    direccion = f"Calle {random.randint(1,100)} #{random.randint(1,100)}-{random.randint(1,100)}"
    ciudad = random.choice(ciudades)
    ingresos = round(random.uniform(20000, 100000), 2)
    score = random.randint(300, 850)
    fecha = random_date(datetime(2020,1,1), datetime(2024,12,31)).strftime('%Y-%m-%d')
    clientes.append((cliente_id, nombre, apellido, edad, genero, estado_civil, educacion, direccion, ciudad, ingresos, score, fecha))

# Write output to a UTF-8 file
def main():
    with open('random_inserts.sql', 'w', encoding='utf-8') as f:
        f.write("-- INSERT INTO clientes\n")
        f.write("INSERT INTO clientes (cliente_id, nombre, apellido, edad, genero, estado_civil, nivel_educacion, direccion, ciudad, ingresos_anuales, score_credicio, fecha_registro) VALUES\n")
        for idx, c in enumerate(clientes):
            row = f"('{c[0]}', '{c[1]}', '{c[2]}', {c[3]}, '{c[4]}', '{c[5]}', '{c[6]}', '{c[7]}', '{c[8]}', {c[9]}, {c[10]}, '{c[11]}')"
            f.write(row + (",\n" if idx < len(clientes)-1 else ";\n"))

        # Generate products for 30 random clients
        tipos_producto = ['Cuenta Ahorros', 'Tarjeta Crédito', 'Cuenta Corriente', 'Préstamo Personal', 'Inversión']
        estados = ['Activo', 'Inactivo']
        productos = []
        selected_clientes = random.sample(clientes, 30)
        prod_id = 2000
        for c in selected_clientes:
            n_products = random.randint(2, 3)
            for _ in range(n_products):
                prod_id += 1
                producto_id = f"PR{prod_id}"
                tipo = random.choice(tipos_producto)
                saldo = round(random.uniform(1000, 30000), 2)
                fecha_apertura = random_date(datetime(2020,1,1), datetime(2024,12,31)).strftime('%Y-%m-%d')
                estado = random.choice(estados)
                productos.append((producto_id, c[0], tipo, saldo, fecha_apertura, estado))

        f.write("\n-- INSERT INTO productos\n")
        f.write("INSERT INTO productos (producto_id, cliente_id, tipo_producto, saldo, fecha_apertura, estado) VALUES\n")
        for idx, p in enumerate(productos):
            row = f"('{p[0]}', '{p[1]}', '{p[2]}', {p[3]}, '{p[4]}', '{p[5]}')"
            f.write(row + (",\n" if idx < len(productos)-1 else ";\n"))

if __name__ == "__main__":
    main()