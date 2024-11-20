-- 1.)Cree una consulta SQL que muestre los nombres y apellidos de los empleados con un salario superior a ¢10,000.00 (5 pts).

SELECT  EMP_NOMBRE,
        EMP_APELLIDO
        FROM EX_EMPLEADOS WHERE EMP_SALARIO >= 10000;
        
-- 2.)Crea una función que tome un salario como parámetro y devuelva el número de empleados que tienen salarios superiores al valor proporcionado (10 pts).

CREATE OR REPLACE FUNCTION F_SALARIO_CAMBIO(P_SALARIO INTEGER) RETURN NUMBER IS V_EMPLEADOS NUMBER;
    BEGIN
        SELECT 
            COUNT(*)
            INTO V_EMPLEADOS
        FROM EX_EMPLEADOS
            WHERE EMP_SALARIO >= P_SALARIO;
    RETURN V_EMPLEADOS;
END;
/

-- 3.)Crea un procedimiento almacenado que aumente el salario de un empleado en un 10%. El procedimiento debe recibir el ID del empleado como parámetro (15 pts).

CREATE OR REPLACE PROCEDURE AUMENTOSALARIO (EMP NUMBER) AS
SALARIO_EMPLEADO DECIMAL(10,2);
SALARIO_NUEVO    DECIMAL(10,2);
BEGIN 
    SELECT EMP_SALARIO
    INTO SALARIO_EMPLEADO
    FROM EX_EMPLEADOS WHERE EMP_ID = EMP;
    SALARIO_NUEVO := SALARIO_EMPLEADO * 1.10;
    UPDATE EX_EMPLEADOS SET EMP_SALARIO = SALARIO_NUEVO WHERE EMP_ID = EMP;
    COMMIT;
END;
/

-- 4.)Diseñe un modelo mutidimensional tipo estrella con las tablas indicadas para el examen (20 pts).

 alter session set "_ORACLE_SCRIPT" = TRUE;
 CREATE USER EXEMPLEADOS_ER IDENTIFIED BY Oracle01 DEFAULT TABLESPACE USERS QUOTA UNLIMITED ON USERS;
 GRANT CONNECT, RESOURCE TO EXEMPLEADOS_ER;

 CREATE USER EXEMPLEADOS_SA IDENTIFIED BY Oracle01 DEFAULT TABLESPACE USERS QUOTA UNLIMITED ON USERS;
 GRANT CONNECT, RESOURCE TO EXEMPLEADOS_SA;
 
 CREATE USER EXEMPLEADOS_DW IDENTIFIED BY Oracle01 DEFAULT TABLESPACE USERS QUOTA UNLIMITED ON USERS;
 GRANT CONNECT, RESOURCE TO EXEMPLEADOS_DW;
 
 --Creación del modelo multidemsional
 
 CREATE TABLE DIM_EX_EMPLEADOS(
    EMP_ID         INTEGER NOT NULL,
    EMP_NOMBRE     VARCHAR(50) NOT NULL,
    EMP_APELLIDO   VARCHAR(50) NOT NULL,
    CONSTRAINT PK_EMP_ID PRIMARY KEY(EMP_ID),
    CONSTRAINT CK_EMP_NOMBRE CHECK(LENGTH(EMP_NOMBRE) >= 1),
    CONSTRAINT CK_EMP_APELLIDO CHECK(LENGTH(EMP_APELLIDO) >= 1)
    );
    
 CREATE TABLE DIM_EX_TIPO_EMPLEADOS(
    TEM_ID              INTEGER NOT NULL,
    TEM_DESCRIPCION     VARCHAR(50) NOT NULL,
    CONSTRAINT PK_TEM_ID PRIMARY KEY(TEM_ID),
    CONSTRAINT CK_TEM_DESCRIPCION CHECK(LENGTH(TEM_DESCRIPCION) >= 5)
    );

 CREATE TABLE FAC_TRANSACCION (
    TRN_EMP_ID      INTEGER NOT NULL,
    TRN_TEM_ID      INTEGER NOT NULL,
    TRN_EMP_SALARIO DECIMAL(20,2) NOT NULL,
    CONSTRAINT PK_TRN PRIMARY KEY (TRN_EMP_ID, TRN_TEM_ID),
    CONSTRAINT FK_TRN_EMP FOREIGN KEY(TRN_EMP_ID) REFERENCES DIM_EX_EMPLEADOS(EMP_ID),
    CONSTRAINT FK_TRN_TEM FOREIGN KEY(TRN_TEM_ID) REFERENCES DIM_EX_TIPO_EMPLEADOS(TEM_ID),
    CONSTRAINT CK_TRN_EMP_SALARIO CHECK(TRN_EMP_SALARIO > 0)
   );
   
-- 5.)Crea el proceso ETL para una de las dimensiones donde se realice la extracción y transformación de datos para el modelo multidimensional creado. No se requiere realizar el ETL del Staging Area. Debe utilizar como mínimo 3 validaciones de los datos y una tabla de bitácora para guardar los posibles errores que genere el proceso. El staging area será las 2 tablas del modelo relacional con las características vistas en clase para construcción del Staging Area (35pts).

  -- Creación del modelo SA
  
  CREATE TABLE EX_EMPLEADOS (
    EMP_ID       VARCHAR2(255),
    EMP_NOMBRE   VARCHAR2(255),
    EMP_APELLIDO VARCHAR2(255),
    EMP_SALARIO  VARCHAR2(255)
    );

  CREATE TABLE EX_TIPO_EMPLEADO(
    TEM_ID          VARCHAR2(255),
    TEM_EMP_ID      VARCHAR2(255),
    TEM_DESCRIPCION VARCHAR2(255)
    );
  -- Permisos
  
    GRANT SELECT ON EXEMPLEADOS_ER.EX_EMPLEADOS TO EXEMPLEADOS_DW;
    GRANT SELECT ON EXEMPLEADOS_ER.EX_TIPO_EMPLEADO TO EXEMPLEADOS_DW;
    ----------------------------------------------------------------------------
    GRANT SELECT, INSERT ON EXEMPLEADOS_SA.EX_EMPLEADOS TO EXEMPLEADOS_DW;
    GRANT SELECT, INSERT ON EXEMPLEADOS_SA.EX_TIPO_EMPLEADO TO EXEMPLEADOS_DW;

  -- Creación del ETL del DW
  
     -- Tabla bitácora
     
        CREATE TABLE EXEMPLEADOS_DW.ERROR_DIM_EX_EMPLEADOS (
            EMP_ID          VARCHAR2(255),
            EMP_NOMBRE      VARCHAR2(255),
            EMP_APELLIDO    VARCHAR2(255),
            EMP_ERROR       VARCHAR2(4000)
        );
        
    -- ETL del DW
    
        CREATE OR REPLACE FUNCTION VALIDA_NUMERO_ENTERO(P_NUMERO VARCHAR2) RETURN CHAR AS
           V_NUMERO NUMBER;
        BEGIN
           V_NUMERO := TO_NUMBER(P_NUMERO);
           IF V_NUMERO = ROUND(V_NUMERO) THEN
              RETURN 'S';
           ELSE
              RETURN 'N';
           END IF;
        EXCEPTION
           WHEN OTHERS THEN
              RETURN 'N';
        END;
        /
        
        CREATE OR REPLACE FUNCTION VALIDA_NUMERO_DECIMAL(P_NUMERO VARCHAR2) RETURN CHAR AS
           V_NUMERO NUMBER(20,2);
        BEGIN
           V_NUMERO := TO_NUMBER(P_NUMERO);
           IF V_NUMERO <> ROUND(V_NUMERO,0) THEN
              RETURN 'S';
           ELSE
              RETURN 'N';
           END IF;
        EXCEPTION
           WHEN OTHERS THEN
              RETURN 'N';
        END;
        /

        -- FunciÓn para validar fecha.
        CREATE OR REPLACE FUNCTION VALIDA_FECHA(P_FECHA VARCHAR2) RETURN CHAR AS
           V_FECHA DATE;
        BEGIN
           V_FECHA := TO_DATE(P_FECHA, 'YYYY-MM-DD');
           RETURN 'S';
        EXCEPTION
           WHEN OTHERS THEN
              RETURN 'N';
        END;
        /

CREATE OR REPLACE PACKAGE EXEMPLEADOS_DW.ETL_DW AS
   PROCEDURE MigrarExempleados;
   PROCEDURE MigrarDatos;
END ETL_DW;
/

CREATE OR REPLACE PACKAGE BODY EXEMPLEADOS_DW.ETL_DW AS
   PROCEDURE MigrarExempleados IS
      V_ERROR  INTEGER;
      V_NUMERO INTEGER;
      V_ERROR_MENSAJE VARCHAR2(4000);
      CURSOR C_DATOS IS
         SELECT EMP.EMP_ID,
                EMP.EMP_NOMBRE,
                EMP.EMP_APELLIDO
           FROM EXEMPLEADOS_SA.EX_EMPLEADOS EMP
          WHERE EMP.EMP_ID NOT IN (SELECT D.EMP_ID FROM EXEMPLEADOS_DW.DIM_EX_EMPLEADOS D)
          ORDER BY EMP.EMP_ID;
   BEGIN
      FOR D_DATOS IN C_DATOS LOOP
         BEGIN
             V_ERROR := 0;
             V_ERROR_MENSAJE := '';
             -----------------------------------------------------------------------
             IF D_DATOS.EMP_ID IS NULL THEN
                V_ERROR := 1;
                V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Código Nulo. ';
             END IF;
             --- Codigo de Cliente no num rico.
             IF VALIDA_NUMERO_ENTERO(D_DATOS.EMP_ID) = 'N' THEN
                V_ERROR := 1;
                V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'CÓdigo no numérico. ';
             ELSE
                V_NUMERO := TO_NUMBER(D_DATOS.EMP_ID);
                --- Codigo de Cliente negativo.
                IF V_NUMERO <= 0 THEN
                   V_ERROR := 1;
                   V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Código Negativo o cero. ';               
                END IF;
             END IF;
             -----------------------------------------------------------------------
             IF D_DATOS.EMP_NOMBRE IS NULL THEN
                V_ERROR := 1;
                V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Nombre Nulo. ';
             END IF;
             IF LENGTH(D_DATOS.EMP_NOMBRE) > 85 THEN
                V_ERROR := 1;
                V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Nombre con longitud mayor. ';
             END IF;
             IF LENGTH(D_DATOS.EMP_NOMBRE) < 1 THEN
                V_ERROR := 1;
                V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Nombre con longitud menor. ';
             END IF;
             IF VALIDA_NUMERO_ENTERO(D_DATOS.EMP_NOMBRE) = 'S' THEN
                V_ERROR := 1;
                V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Descripción numérica. ';
             END IF;
             -----------------------------------------------------------------------
             IF D_DATOS.EMP_APELLIDO IS NULL THEN
                V_ERROR := 1;
                V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Apellido Nulo. ';
             END IF;
             IF LENGTH(D_DATOS.EMP_APELLIDO) > 85 THEN
                V_ERROR := 1;
                V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Apellido con longitud mayor. ';
             END IF;
             IF LENGTH(D_DATOS.EMP_APELLIDO) < 1 THEN
                V_ERROR := 1;
                V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Apellido con longitud menor. ';
             END IF;
             IF VALIDA_NUMERO_ENTERO(D_DATOS.EMP_APELLIDO) = 'S' THEN
                V_ERROR := 1;
                V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Descripción numérica. ';
             END IF;
             -----------------------------------------------------------------------
             IF V_ERROR = 0 THEN
                INSERT
                  INTO EXEMPLEADOS_DW.DIM_EX_EMPLEADOS (EMP_ID, EMP_NOMBRE, EMP_APELLIDO)
                                        VALUES (D_DATOS.EMP_ID, D_DATOS.EMP_NOMBRE, D_DATOS.EMP_APELLIDO);
             ELSE
                INSERT INTO EXEMPLEADOS_DW.ERROR_EX_EMPLEADOS (EMP_ID, EMP_NOMBRE, EMP_APELLIDO, EMP_ERROR)
                                                   VALUES (D_DATOS.EMP_ID, D_DATOS.EMP_NOMBRE, D_DATOS.EMP_APELLIDO, V_ERROR_MENSAJE);            
             END IF;
             EXCEPTION
                WHEN OTHERS THEN
                    INSERT INTO EXEMPLEADOS_DW.ERROR_EX_EMPLEADOS (EMP_ID, EMP_NOMBRE, EMP_APELLIDO, EMP_ERROR)
                                                       VALUES (D_DATOS.EMP_ID, D_DATOS.EMP_NOMBRE, D_DATOS.EMP_APELLIDO, 'Error al insertar');
         END;
      END LOOP;
   END;
   -- Migración de los datos.
   PROCEDURE MigrarDatos IS
      BEGIN
         MigrarExempleados;
      END;
END ETL_DW;
/

EXECUTE ETL_DW.MigrarDatos;

COMMIT;