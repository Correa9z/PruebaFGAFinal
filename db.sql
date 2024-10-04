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
    caso VARCHAR(400),
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
    caso VARCHAR(400),
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
    caso VARCHAR(400),
    PRIMARY KEY(id)
);


-- Generación del trigger de Departamentos -----------------

DELIMITER $$

CREATE TRIGGER departamento_insert
BEFORE INSERT ON departamentos_temp
FOR EACH ROW
BEGIN
    DECLARE departamento_existente INT;

    -- Verificar si el departamento ya existe
    SELECT COUNT(*) INTO departamento_existente
    FROM departamentos 
    WHERE nombre = NEW.nombre;

    IF departamento_existente > 0 THEN
        -- Registrar en el log que el departamento ya existe
        SET NEW.caso = CONCAT("Error, ", NEW.nombre, ' ya existe en la BD.');
    ELSE
        -- Insertar el nuevo departamento
        INSERT INTO departamentos (nombre) VALUES (NEW.nombre);

        -- Registrar en el log que se insertó el departamento
        SET NEW.caso = CONCAT("Info, ", NEW.nombre, ' se registró correctamente.');
    END IF;
END $$

DELIMITER ;



-- Generación del trigger de Empleados -----------------
DELIMITER $$

CREATE TRIGGER empleado_insert
BEFORE INSERT ON empleados_temp
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
        
        SET NEW.caso = CONCAT("Info,",NEW.nombre,' ',NEW.identificacion,' ',NEW.departamento ,' se inserto corretamente.');

    ELSE
        IF departamento_existente IS NULL AND empleado_existente = 0 THEN

            SET NEW.caso = CONCAT("Error,",NEW.nombre,' ',NEW.identificacion,' ',NEW.departamento ,' No tiene un departamento existente.');
        
        ELSE
            SET NEW.caso = CONCAT("Error,",NEW.nombre,' ',NEW.identificacion,' ',NEW.departamento ,' ya se encuentra en la BD.');
        END IF;
    END IF;
END $$

DELIMITER ;

-- Generación del trigger de Proyectos -----------------
DELIMITER $$

CREATE TRIGGER proyecto_insert
BEFORE INSERT ON proyectos_temp
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

        SET NEW.caso = CONCAT("Info,",NEW.nombre,' ',NEW.empleado,' se inserto corretamente.');

    ELSE
        IF empleado_existente IS NULL AND proyecto_existente = 0 THEN

            SET NEW.caso = CONCAT("Error,",NEW.nombre,' ',NEW.empleado,' No tiene un empleado existente.');
        
        ELSE
            SET NEW.caso = CONCAT("Info,",NEW.nombre,' ',NEW.empleado,' Ya se encuentra en la BD.');
        END IF;
    END IF;
END $$

DELIMITER ;