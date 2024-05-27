CREATE TABLE IF NOT EXISTS Notification_Type
(
    Id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    Type VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS Action_Type
(
    Id UUID DEFAULT gen_random_uuid() PRIMARY KEY UNIQUE,
    Type VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS Notification_State
(
    Id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    State VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS Notification
(
    Id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    Type VARCHAR(50),
    Created timestamp without time zone,
    Reviewd_By uuid,
    Reviewed_At timestamp without time zone,
    State VARCHAR(50),
    Patient_Id VARCHAR(50),
    Names VARCHAR(100),
    Golden_Id VARCHAR(50),
    Score Numeric
);

CREATE TABLE IF NOT EXISTS Action
(
    Id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    Notification_Id UUID,
    Action_Type_Id UUID,
    Date date,
    CONSTRAINT FK_Notification
      FOREIGN KEY(Notification_Id) 
	    REFERENCES Notification(Id),
    CONSTRAINT FK_Action_Type
      FOREIGN KEY(Action_Type_Id) 
	    REFERENCES Action_Type(Id)
);

CREATE TABLE IF NOT EXISTS Match
(
    Notification_Id UUID,
    Score Numeric,
    Golden_Id VARCHAR(50),
    CONSTRAINT FK_Notification
      FOREIGN KEY(Notification_Id) 
	    REFERENCES Notification(Id)
);

CREATE TABLE IF NOT EXISTS candidates
(
    Notification_Id UUID,
    Score Numeric,
    Golden_Id VARCHAR(50),
    CONSTRAINT FK_Notification
      FOREIGN KEY(Notification_Id) 
	    REFERENCES Notification(Id)
);

CREATE TABLE IF NOT EXISTS users
(
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY UNIQUE,
    given_name VARCHAR(255),
    family_name VARCHAR(255),
    email VARCHAR(255) UNIQUE,
    username VARCHAR(255) UNIQUE
);

CREATE TABLE IF NOT EXISTS mpi_matching_output(
    dwh_id UUID DEFAULT gen_random_uuid() PRIMARY KEY UNIQUE,
    golden_id varchar(32),
    encounter_id varchar(32),
    pkv varchar(100),
    gender varchar(32),
    phonetic_given_name varchar(100),
    phonetic_family_name varchar(100),
    dob varchar(32),
    nupi varchar(32),
    ccc_number varchar(150),
    site_code varchar(32),
    docket varchar(32),
    patient_pk varchar(32),
    patient_pk_hash varchar(255)
    );

INSERT INTO Notification_State(State)
VALUES ('OPEN'), ('CLOSED');

INSERT INTO Notification_Type(Type)
VALUES ('ABOVE_THRESHOLD'), ('BELOW_THRESHOLD'), ('MARGIN'), ('UPDATE');