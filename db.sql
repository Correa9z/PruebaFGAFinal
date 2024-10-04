-- Creación de la base de datos
CREATE DATABASE pruebafga;

-- Referencia/uso de la base de datos
USE pruebafga;

-- Creación de la tablas respecto a Departamentos ------
CREATE TABLE departamentos(
    id INT AUTO_INCREMENT,
    nombre VARCHAR(100) UNIQUE,
    PRIMARY KEY(id)
);

CREATE TABLE departamentos_temp(
    id INT AUTO_INCREMENT,
    nombre VARCHAR(100),
    PRIMARY KEY(id)
);

CREATE TABLE departamentos_log(
    id INT AUTO_INCREMENT,
    fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    mensaje VARCHAR(100),
    PRIMARY KEY(id)
);

-- Creación de la tablas respecto a Empleados ------
CREATE TABLE empleados(
    id INT AUTO_INCREMENT,
    nombre VARCHAR(100),
    identificacion VARCHAR(100) UNIQUE,
    departamento_id INT,
    PRIMARY KEY(id),
    FOREIGN KEY(departamento_id) REFERENCES departamentos(id)  
);

CREATE TABLE empleados_temp(
    id INT AUTO_INCREMENT,
    nombre VARCHAR(100),
    identificacion VARCHAR(100),
    departamento VARCHAR(100),
    PRIMARY KEY(id)
);

CREATE TABLE empleados_log(
    id INT AUTO_INCREMENT,
    fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    mensaje VARCHAR(100),
    PRIMARY KEY(id)
);


-- Creación de la tablas respecto a Proyectos ------
CREATE TABLE proyectos(    
    id INT AUTO_INCREMENT,
    nombre VARCHAR(100) UNIQUE,
    empleado_id INT,
    PRIMARY KEY(id),
    FOREIGN KEY(empleado_id) REFERENCES empleados(id)  
);

CREATE TABLE proyectos_temp(    
    id INT AUTO_INCREMENT,
    nombre VARCHAR(100),
    empleado VARCHAR(100),
    PRIMARY KEY(id)
);

CREATE TABLE proyectos_log(    
    id INT AUTO_INCREMENT,
    fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    mensaje VARCHAR(100),
    PRIMARY KEY(id) 
);


-- Generación del trigger de Departamentos -----------------

DELIMITER $$

CREATE TRIGGER departamento_insert
AFTER INSERT ON departamentos_temp
FOR EACH ROW
BEGIN
    DECLARE departamento_existente INT;

    -- Verificar si el departamento ya existe
    SELECT COUNT(*) INTO departamento_existente
    FROM departamentos 
    WHERE nombre = NEW.nombre;

    IF departamento_existente > 0 THEN
        -- Registrar en el log que el departamento ya existe
        INSERT INTO departamentos_log (mensaje)
        VALUES (CONCAT('El departamento ', NEW.nombre, ' ya existe en la BD.'));
    ELSE
        -- Insertar el nuevo departamento
        INSERT INTO departamentos (nombre) VALUES (NEW.nombre);

        -- Registrar en el log que se insertó el departamento
        INSERT INTO departamentos_log (mensaje)
        VALUES (CONCAT('Se insertó el departamento ', NEW.nombre, ' correctamente.'));
    END IF;
END $$

DELIMITER ;


-- Generación del trigger de Empleados -----------------
DELIMITER $$

CREATE TRIGGER empleado_insert
AFTER INSERT ON empleados_temp
FOR EACH ROW
BEGIN
    DECLARE departamento_existente INT;
    DECLARE empleado_existente INT;

    -- Verificar si el departamento ya existe
    SELECT id INTO departamento_existente
    FROM departamentos 
    WHERE nombre = NEW.departamento;

    SELECT COUNT(*) INTO empleado_existente
    FROM empleados 
    WHERE identificacion = NEW.identificacion;

    IF departamento_existente IS NOT NULL AND empleado_existente = 0 THEN
        
        INSERT INTO empleados (nombre, identificacion, departamento_id) VALUES (NEW.nombre, NEW.identificacion, departamento_existente);

        INSERT INTO empleados_log (mensaje)
        VALUES (CONCAT('Se insertó el empleado ', NEW.nombre,' ',NEW.identificacion,' ',NEW.departamento, ' correctamente.'));

    ELSE
        IF departamento_existente IS NULL AND empleado_existente = 0 THEN
        
            INSERT INTO empleados_log (mensaje)
            VALUES (CONCAT('El empleado ', NEW.nombre,' ',NEW.identificacion,' ',NEW.departamento, ' no tiene un departamento existente.'));
        ELSE
            INSERT INTO empleados_log (mensaje)
            VALUES (CONCAT('El empleado ', NEW.nombre,' ',NEW.identificacion,' ',NEW.departamento, ' ya esta en la BD.'));
        END IF;
    END IF;
END $$

DELIMITER ;



-- Generación del trigger de Proyectos -----------------
DELIMITER $$

CREATE TRIGGER proyecto_insert
AFTER INSERT ON proyectos_temp
FOR EACH ROW
BEGIN
    DECLARE empleado_existente INT;
    DECLARE proyecto_existente INT;

    -- Verificar si el departamento ya existe
    SELECT id INTO empleado_existente
    FROM empleados 
    WHERE nombre = NEW.empleado LIMIT 1;

    SELECT COUNT(*) INTO proyecto_existente
    FROM proyectos 
    WHERE nombre = NEW.nombre;

    IF empleado_existente IS NOT NULL AND proyecto_existente = 0 THEN
        
        INSERT INTO proyectos (nombre, empleado_id) VALUES (NEW.nombre, empleado_existente);

        INSERT INTO proyectos_log (mensaje)
        VALUES (CONCAT('Se insertó el proyecto ', NEW.nombre,' ',NEW.empleado, ' correctamente.'));

    ELSE
        IF empleado_existente IS NULL AND proyecto_existente = 0 THEN
        
            INSERT INTO proyectos_log (mensaje)
            VALUES (CONCAT('El proyecto ', NEW.nombre,' ',NEW.empleado, ' no tiene un empleado existente.'));
        ELSE
            INSERT INTO proyectos_log (mensaje)
            VALUES (CONCAT('El proyecto ', NEW.nombre,' ',NEW.empleado, ' ya esta en la BD.'));
        END IF;
    END IF;
END $$

DELIMITER ;