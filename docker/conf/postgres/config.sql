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
    Type_Id uuid,
    Created date,
    Reviewd_By uuid,
    Reviewed_At timestamp without time zone,
    State_Id uuid,
    Patient_Id VARCHAR(50),
    Names VARCHAR(100)
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

INSERT INTO Notification_State(State)
VALUES ('New'), ('Seen'), ('Actioned'), ('Accepted'), ('Pending');

INSERT INTO Notification_Type(Type)
VALUES ('THRESHOLD'), ('MARGIN'), ('UPDATE');


CREATE TABLE IF NOT EXISTS dwh
(
     dwh_id        UUID DEFAULT gen_random_uuid() PRIMARY KEY UNIQUE,
     golden_id     VARCHAR(32),
     encounter_id  VARCHAR(32),
     pkv           VARCHAR(150),
     gender        VARCHAR(32),
     dob           VARCHAR(32),
     nupi          VARCHAR(32),
     site_code     VARCHAR(32),
     patient_pk    VARCHAR(32),
     ccc_number    VARCHAR(150)
     
);


CREATE INDEX IF NOT EXISTS idx_dwh_gid ON dwh(golden_id);
CREATE INDEX IF NOT EXISTS idx_dwh_eid ON dwh(encounter_id);

\dt;
