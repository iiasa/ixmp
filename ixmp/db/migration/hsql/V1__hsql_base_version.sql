--
-- create ixmp schema objects (tables, views, sequences)
--
-- sequences
CREATE SEQUENCE ANNOTATION_SEQ as bigint;
CREATE SEQUENCE CHANGELOG_SEQ as bigint;
CREATE SEQUENCE IAMC_KEY_SEQ as bigint;
CREATE SEQUENCE MODEL_SEQ as bigint;
CREATE SEQUENCE RUN_SEQ as bigint;
CREATE SEQUENCE SCENARIO_SEQ as bigint;
CREATE SEQUENCE UNIT_SEQ as bigint start with 100;
CREATE SEQUENCE IAMC_TS_SEQ as bigint;

--
-- tables

--
-- schema configuration table (version tags)

create table SCHEMA_CONFIG (
    PARAM VARCHAR(64) not null,
    PARTYPE VARCHAR(8) not null,
    STRVALUE VARCHAR(128),
    NUMVALUE DOUBLE,
    NOTE VARCHAR(256)
);

insert into SCHEMA_CONFIG (param, partype, numvalue) values ('VERSION_MAJOR','INTEGER', 0);
insert into SCHEMA_CONFIG (param, partype, numvalue) values ('VERSION_MINOR','INTEGER', 1);

--
-- app configuration and file upload/import stuff

create table APP_AUDIT  (	
 USR VARCHAR(64)  not null, 
 MODEL VARCHAR(64)  not null, 
 SCEN VARCHAR(64)  not null, 
 ACTION VARCHAR(64)  , 
 INFO VARCHAR(512)  ,
 "WHEN" TIMESTAMP default now() not null
);

create table APP_FILES  (	
 ID INTEGER not null, 
 USR VARCHAR(64) not null, 
 GRP VARCHAR(64) not null, 
 NAME VARCHAR(256) not null, 
 "TIMESTAMP" TIMESTAMP default now() not null, 
 FILESIZE DOUBLE DEFAULT 0, 
 STATUS VARCHAR(64)
 ,	PRIMARY KEY ("ID")
);

create table APP_IMPORT_HISTORY   (
 USR VARCHAR(64) not null, 
 MODEL VARCHAR(64) not null, 
 SCENARIO VARCHAR(64) not null , 
 "TIMESTAMP" TIMESTAMP default now() not null, 
 FILENAME VARCHAR(128), 
 INFO VARCHAR(64)
);

create table APP_IMPORT_QUEUE (
 USR VARCHAR(64)  not null, 
 FILENAME VARCHAR(256) not null, 
 STATE VARCHAR(64) default 'NEW', 
 "TIMESTAMP" TIMESTAMP default now()
 ,	PRIMARY KEY ("USR", "FILENAME")
);

create table APP_CONFIG (
 PARAM VARCHAR(64)  not null,
 PARTYPE VARCHAR(8) default 'STRING' not null,
 STRVALUE VARCHAR(128)  ,
 NUMVALUE INTEGER  ,
 NOTE VARCHAR(256)
 , PRIMARY KEY ( PARAM ) 
);

create table APP_USER (
 LOGIN VARCHAR(128) not null,
 PASSWORD VARCHAR(128) default 'iimq9WhxKcHSe5BXj8M+zEnQadw=' not null,
 EMAIL VARCHAR(128)  ,
 REALNAME VARCHAR(128)  ,
 COUNTRY VARCHAR(128)  ,
 ORGANIZATION VARCHAR(128)  ,
 STREET VARCHAR(256)  ,
 TEMPKEY VARCHAR(128)   
 , PRIMARY KEY ( LOGIN ) 
);

create table APP_USER_CONFIG (
  LOGIN VARCHAR(128) not null,
  PARAM VARCHAR(64) not null,
  PARTYPE VARCHAR(8) default 'STRING' not null,
  STRVALUE VARCHAR(256), 
  NUMVALUE DOUBLE, 
  NOTE VARCHAR(256) 
 , PRIMARY KEY ("LOGIN", "PARAM")
);
--
-- ixmp structure

create table MODEL (
 ID INTEGER  not null,
 NAME VARCHAR(64)  not null 
 , PRIMARY KEY ( ID ) 
);


create table SCENARIO (
 ID INTEGER  not null,
 NAME VARCHAR(64)  not null 
 , PRIMARY KEY ( ID ) 
);

create table RUN (
 ID INTEGER  not null,
 MODEL_ID INTEGER  not null,
 SCEN_ID INTEGER  not null,
 CRE_USER VARCHAR(64) default '(unknown)' not null,
 CRE_DATE TIMESTAMP default now() not null,
 UPD_USER VARCHAR(64)  ,
 UPD_DATE TIMESTAMP  ,
 SCHEME VARCHAR(20)  ,
 STATUS INTEGER default 0 not null,
 VERSION INTEGER default 0 not null,
 LOCK_USER VARCHAR(64)  ,
 LOCK_DATE TIMESTAMP  ,
 SEQ_ITEM_ID INTEGER,   
 ANNOTATION VARCHAR(264)
 , PRIMARY KEY ( ID ) 
);

create table RUN_DEFAULT (
 MODEL_ID INTEGER  not null,
 SCEN_ID INTEGER  not null,
 ID INTEGER  not null 
 , PRIMARY KEY ( MODEL_ID,SCEN_ID ) 
);

create table ANNOTATION_LOG (
 ANNOTATIONID INTEGER not null,
 STATUS VARCHAR(128) not null,
 RUNID INTEGER,
 SCRIPT VARCHAR(128) not null,
 CRE_USER VARCHAR(128) default '(unknown)' not null,
 CRE_DATE TIMESTAMP default now() not null,
 UPD_USER VARCHAR(64)  ,
 UPD_DATE TIMESTAMP  ,
 TEXT VARCHAR(512)  ,
 INTERFACE VARCHAR(20)   
 , PRIMARY KEY ( ANNOTATIONID ) 
);

create table CHANGE_LOG (
 ANNOTATIONID INTEGER  not null,
 RUNID INTEGER  not null,
 OPERATION VARCHAR(64)  not null,
 ITEM VARCHAR(64)  not null,
 KEY VARCHAR(512)  ,
 VAL_PREV DOUBLE  ,
 VAL_NEW DOUBLE  ,
 SEQ INTEGER   
  
);

--
-- IAMC timeseries structure and nodes, units, etc. 

create table IX_UNIT (
 ID INTEGER  not null,
 NAME VARCHAR(64)  not null,
 CRE_USER VARCHAR(64), 
 CRE_DATE TIMESTAMP default now() not null,
 SCHEME VARCHAR(64)
 , PRIMARY KEY ( ID ) 
 , CONSTRAINT UNIT_NAME_INDEX UNIQUE ( NAME )
);

create table IAMC_KEY (
 KEYID INTEGER  not null,
 KEYSTRING VARCHAR(256)  not null,
 UNITID INTEGER  not null,
 CRE_USER VARCHAR(64)  not null,
 CRE_DATE TIMESTAMP  not null,
 SCHEME VARCHAR(128)   
 , PRIMARY KEY ( KEYID ) 
);

create table IAMC_NODES (
 ID INTEGER  not null,
 HIERARCHY VARCHAR(20)  not null,
 NAME VARCHAR(64)  not null,
 PARENT VARCHAR(64)  not null 
 , PRIMARY KEY ( ID ) 
);

create table IAMC_NODES_SYNONYM (
 NODE_ID INTEGER  not null,
 NODE_SYNONYM VARCHAR(64)  not null 
 , PRIMARY KEY ( NODE_SYNONYM ) 
);

create table IAMC_TSDATA (
 TSID INTEGER not null,
 YEAR INTEGER  not null,
 VALUE DOUBLE  not null
 , PRIMARY KEY ( TSID,YEAR ) 
);

create table IAMC_TSINFO (
 TSID INTEGER not null,
 RUNID INTEGER  not null,
 NODE INTEGER  not null,
 KEY INTEGER  not null,
 TIME INTEGER  not null,
 META INTEGER  default 0 not null
 , PRIMARY KEY ( TSID,RUNID,NODE,KEY,TIME ) 
 , CONSTRAINT IAMC_TSINFO_TSID_UK UNIQUE ( TSID )
);

--
-- IXdatastructure tables 

create table IX_KEY (
 RUNID INTEGER  not null,
 KEYID INTEGER  not null,
 KEY VARCHAR(40)  not null 
 , PRIMARY KEY ( RUNID,KEYID ) 
);

create table IX_COMMENT (
 COMID INTEGER  not null,
 COMSTRING VARCHAR(512)  not null,
 RUNID INTEGER  not null 
 , PRIMARY KEY ( COMID,RUNID ) 
);

create table IX_IDXSET (
 RUNID INTEGER  not null,
 NAME VARCHAR(64)  not null,
 ELE_BLOB BLOB  ,
 ITEMID INTEGER  not null,
 COM_BLOB BLOB   
 , PRIMARY KEY ( RUNID,NAME ) 
);


create table IX_SET (
 RUNID INTEGER  not null,
 NAME VARCHAR(64)  not null,
 DIM INTEGER default null ,
 ITEMID INTEGER   
 , PRIMARY KEY ( RUNID,NAME ) 
);

create table IX_SET_DIM (
 RUNID INTEGER  not null,
 NAME VARCHAR(64)  not null,
 IDX INTEGER  not null,
 IDX_SET VARCHAR(64)  not null,
 IDX_NAME VARCHAR(64)  not null 
 , PRIMARY KEY ( RUNID,NAME,IDX ) 
);

create table IX_SET_BLOBSTORE (
 RUNID INTEGER  not null,
 NAME VARCHAR(256)  not null,
 ELE_BLOB BLOB  ,
 COM_BLOB BLOB   
 , PRIMARY KEY ( RUNID,NAME ) 
);

create table IX_PAR (
 RUNID INTEGER  not null,
 NAME VARCHAR(64)  not null,
 DIM INTEGER default 0 not null,
 ITEMID INTEGER   
 , PRIMARY KEY ( RUNID,NAME ) 
);

create table IX_PAR_DIM (
 RUNID INTEGER  not null,
 NAME VARCHAR(64)  not null,
 IDX INTEGER  not null,
 IDX_SET VARCHAR(64)  not null,
 IDX_NAME VARCHAR(64)  not null 
 , PRIMARY KEY ( RUNID,NAME,IDX ) 
);

create table IX_PAR_BLOBSTORE (
 RUNID INTEGER  not null,
 NAME VARCHAR(256)  not null,
 ELE_BLOB BLOB  ,
 COM_BLOB BLOB   
 , PRIMARY KEY ( RUNID,NAME ) 
);

create table IX_VAR (
 RUNID INTEGER  not null,
 NAME VARCHAR(64)  not null,
 DIM INTEGER default 0 not null,
 ITEMID INTEGER  not null 
 , PRIMARY KEY ( RUNID,NAME ) 
);


create table IX_VAR_DIM (
 RUNID INTEGER  not null,
 NAME VARCHAR(64)  not null,
 IDX INTEGER  not null,
 IDX_SET VARCHAR(64)  not null,
 IDX_NAME VARCHAR(64)  not null 
 , PRIMARY KEY ( RUNID,NAME,IDX ) 
);

create table IX_VAR_BLOBSTORE (
 RUNID INTEGER  not null,
 NAME VARCHAR(256)  not null,
 ELE_BLOB BLOB  ,
 COM_BLOB BLOB   
 , PRIMARY KEY ( RUNID,NAME ) 
);

create table IX_EQU (
 RUNID INTEGER  not null,
 NAME VARCHAR(64)  not null,
 DIM INTEGER  not null,
 ITEMID INTEGER  not null 
 , PRIMARY KEY ( RUNID,NAME ) 
);

create table IX_EQU_DIM (
 RUNID INTEGER  not null,
 NAME VARCHAR(64)  not null,
 IDX INTEGER  not null,
 IDX_SET VARCHAR(64)  not null,
 IDX_NAME VARCHAR(64)  not null 
 , PRIMARY KEY ( RUNID,NAME,IDX ) 
);

create table IX_EQU_BLOBSTORE (
 RUNID INTEGER  not null,
 NAME VARCHAR(256)  not null,
 ELE_BLOB BLOB  ,
 COM_BLOB BLOB   
 , PRIMARY KEY ( RUNID,NAME ) 
);

alter table RUN add foreign key ( MODEL_ID ) references MODEL ( ID ) on update CASCADE on delete RESTRICT;
alter table RUN add foreign key ( SCEN_ID ) references SCENARIO ( ID ) on update CASCADE on delete RESTRICT;
alter table RUN_DEFAULT add foreign key ( ID ) references RUN ( ID ) on update CASCADE on delete CASCADE;
alter table ANNOTATION_LOG add foreign key ( RUNID ) references RUN ( ID ) on update CASCADE on delete CASCADE;
alter table CHANGE_LOG add foreign key ( ANNOTATIONID ) references ANNOTATION_LOG ( ANNOTATIONID ) on update CASCADE on delete RESTRICT;
alter table CHANGE_LOG add foreign key ( RUNID ) references RUN ( ID ) on update CASCADE on delete CASCADE;
alter table IAMC_KEY add foreign key ( UNITID ) references IX_UNIT ( ID ) on update CASCADE on delete RESTRICT;
alter table IAMC_NODES_SYNONYM add foreign key ( NODE_ID ) references IAMC_NODES ( ID ) on update CASCADE on delete RESTRICT;
alter table IAMC_TSDATA add foreign key ( TSID ) references IAMC_TSINFO ( TSID ) on update CASCADE on delete CASCADE;
alter table IX_KEY add foreign key ( RUNID ) references RUN ( ID ) on update CASCADE on delete CASCADE;
alter table IX_COMMENT add foreign key ( RUNID ) references RUN ( ID ) on update CASCADE on delete CASCADE;
alter table IX_IDXSET add foreign key ( RUNID ) references RUN ( ID ) on update CASCADE on delete CASCADE;
alter table IX_SET add foreign key ( RUNID ) references RUN ( ID ) on update CASCADE on delete CASCADE;
alter table IX_SET_DIM add foreign key ( RUNID,NAME ) references IX_SET ( RUNID,NAME ) on update CASCADE on delete CASCADE;
alter table IX_SET_DIM add foreign key ( RUNID,IDX_SET ) references IX_IDXSET ( RUNID,NAME ) on update CASCADE on delete CASCADE;
alter table IX_SET_BLOBSTORE add foreign key ( RUNID,NAME ) references IX_SET ( RUNID,NAME ) on update CASCADE on delete CASCADE;
alter table IX_PAR add foreign key ( RUNID ) references RUN ( ID ) on update CASCADE on delete CASCADE;
alter table IX_PAR_DIM add foreign key ( RUNID,NAME ) references IX_PAR ( RUNID,NAME ) on update CASCADE on delete CASCADE;
alter table IX_PAR_DIM add foreign key ( RUNID,IDX_SET ) references IX_IDXSET ( RUNID,NAME ) on update CASCADE on delete CASCADE;
alter table IX_PAR_BLOBSTORE add foreign key ( RUNID,NAME ) references IX_PAR ( RUNID,NAME ) on update CASCADE on delete CASCADE;
alter table IX_VAR add foreign key ( RUNID ) references RUN ( ID ) on update CASCADE on delete CASCADE;
alter table IX_VAR_DIM add foreign key ( RUNID,NAME ) references IX_VAR ( RUNID,NAME ) on update CASCADE on delete CASCADE;
alter table IX_VAR_DIM add foreign key ( RUNID,IDX_SET ) references IX_IDXSET ( RUNID,NAME ) on update CASCADE on delete CASCADE;
alter table IX_VAR_BLOBSTORE add foreign key ( RUNID,NAME ) references IX_VAR ( RUNID,NAME ) on update CASCADE on delete CASCADE;
alter table IX_EQU add foreign key ( RUNID ) references RUN ( ID ) on update CASCADE on delete CASCADE;
alter table IX_EQU_DIM add foreign key ( RUNID,NAME ) references IX_EQU ( RUNID,NAME ) on update CASCADE on delete CASCADE;
alter table IX_EQU_DIM add foreign key ( RUNID,IDX_SET ) references IX_IDXSET ( RUNID,NAME ) on update CASCADE on delete CASCADE;
alter table IX_EQU_BLOBSTORE add foreign key ( RUNID,NAME ) references IX_EQU ( RUNID,NAME ) on update CASCADE on delete CASCADE;

--
--views

--view RUN_OVERVIEW;
CREATE VIEW RUN_OVERVIEW (ID, MODEL, SCENARIO, CRE_USER, CRE_DATE, UPD_USER, UPD_DATE, LOCK_USER, LOCK_DATE, SCHEME, STATUS, VERSION, ANNOTATION, ISDEFAULT) 
AS
select r.id, m.name model, s.name scenario, 
r.cre_user, r.cre_date, r.upd_user, r.upd_date, r.lock_user, r.lock_date,
r.scheme, r.status, r.version version, r.annotation
, case cast(d.id as BOOLEAN) when true then 'True' else ' ' end isdefault
from run r
inner join model m on m.id = r.model_id
inner join scenario s on s.id = r.scen_id
left join run_default d on (d.MODEL_ID = m.ID and d.SCEN_ID = s.ID and d.ID = r.ID )
order by model, scenario, d.ID, version desc;

--view IAMC_OVERVIEW
CREATE VIEW IAMC_OVERVIEW (MODEL, SCENARIO, REGION, VARIABLE, UNIT, TIME, YEAR, VALUE, META, VERSION, ISDEFAULT) 
AS
select m.name, s.name, n.name, k.keystring, u.name, i.time, d.year, d.value, i.meta, r.version, 
case cast(rd.id as BOOLEAN) when true then 'True' else ' ' end isdefault
from iamc_key k
inner join ix_unit u on (u.id = k.unitid)
inner join iamc_tsinfo i on (i.key = k.keyid)
inner join run r on (r.id = i.runid)
left join run_default rd on (rd.ID = r.ID)
inner join model m on m.id = r.model_id
inner join scenario s on s.id = r.scen_id
inner join iamc_nodes n on (i.node = n.id)
inner join iamc_tsdata d on (i.tsid = d.tsid);


--
-- prepare units and nodes data

-- admin user (to bootstrap user administration)
insert into app_user (login,password,realname) values ('admin','pMuy85M8UBbafoP9E1q4pItnv2E=','Administrator');

-- ix_unit
insert into ix_unit (id,name) values (-1 ,'???');
insert into ix_unit (id,name) values(0, 'y');
insert into ix_unit (id,name) values(1, '%');
insert into ix_unit (id,name) values(2, 'GWa');
insert into ix_unit (id,name) values(3, 'USD/GWa');
insert into ix_unit (id,name) values(4, 'kg/kWa');
insert into ix_unit (id,name) values(5, 'kg');
insert into ix_unit (id,name) values(6, 'USD/kg');
insert into ix_unit (id,name) values(7, 'USD');
insert into ix_unit (id,name) values(8, 'cases');
insert into ix_unit (id,name) values(9, 'km');
insert into ix_unit (id,name) values(10, 'USD/km');
insert into ix_unit (id,name) values(11, 'GW');
insert into ix_unit (id,name) values(12, 'G$');
insert into ix_unit (id,name) values(13, 'T$');
insert into ix_unit (id,name) values(14, '-');
insert into ix_unit (id,name) values(15, 'USD/kWa');
insert into ix_unit (id,name) values(16, 'MW');
insert into ix_unit (id,name) values(17, 'MWa');
insert into ix_unit (id,name) values(18, 'USD/tC');
insert into ix_unit (id,name) values(19, 'USD/tCO2');
insert into ix_unit (id,name) values(20, 't');
insert into ix_unit (id,name) values(21, 'tC');
insert into ix_unit (id,name) values(22, 'tCO2');
-- note that starting value of the sequence UNIT_SEQ must be increased when adding entries here 


-- iamc_nodes
insert into iamc_nodes(id, hierarchy, name, parent) values ('0','global','World','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('1','region (R11)','North America (R11)','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('2','region (R11)','Latin America (R11)','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('3','region (R11)','Western Europe (R11)','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('4','region (R11)','Eastern Europe (R11)','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('5','region (R11)','Former Soviet Union (R11)','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('6','region (R11)','Middle East and Africa (R11)','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('7','region (R11)','Subsaharan Africa (R11)','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('8','region (R11)','Centrally planned Asia (R11)','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('9','region (R11)','Pacific OECD (R11)','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('10','region (R11)','Pacific Asia (R11)','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('11','region (R11)','South Asia (R11)','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('12','country','Canada','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('13','country','Sao Tome and Principe','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('14','country','Turkmenistan','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('15','country','Saint Helena','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('16','country','Lithuania','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('17','country','Cambodia','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('18','country','Ethiopia','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('19','country','Aruba','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('20','country','Swaziland','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('21','country','Palestine','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('22','country','Argentina','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('23','country','Bolivia','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('24','country','Bahamas, The','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('25','country','Burkina Faso','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('26','country','Ghana','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('27','country','Saudi Arabia','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('28','country','Cape Verde','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('29','country','Slovenia','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('30','country','Guatemala','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('31','country','Bosnia and Herzegovina','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('32','country','Guinea','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('33','country','Jordan','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('34','country','Congo, Rep.','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('35','country','Spain','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('36','country','Liberia','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('37','country','Netherlands','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('38','country','Jamaica','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('39','country','Oman','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('40','country','Tanzania','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('41','country','Albania','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('42','country','Gabon','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('43','country','Niue','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('44','country','New Zealand','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('45','country','Yemen','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('46','country','Pakistan','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('47','country','Greenland','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('48','country','Samoa','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('49','country','Slovak Republic','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('50','country','United Arab Emirates','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('51','country','Guam','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('52','country','Kosovo','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('53','country','India','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('54','country','Azerbaijan','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('55','country','Madagascar','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('56','country','Lesotho','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('57','country','Saint Vincent and the Grenadines','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('58','country','Kenya','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('59','country','South Korea','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('60','country','Belarus','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('61','country','Tajikistan','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('62','country','Turkey','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('63','country','Afghanistan','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('64','country','Bangladesh','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('65','country','Mauritania','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('66','country','Solomon Islands','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('67','country','Turks and Caicos Islands','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('68','country','Saint Lucia','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('69','country','Cyprus','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('70','country','French Polynesia','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('71','country','France','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('72','country','Bermuda','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('73','country','Namibia','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('74','country','Somalia','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('75','country','Peru','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('76','country','Laos','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('77','country','Nauru','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('78','country','Seychelles','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('79','country','Norway','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('80','country','Cote d''Ivoire','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('82','country','Cook Islands','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('83','country','Benin','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('84','country','Western Sahara','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('85','country','Cuba','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('86','country','Cameroon','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('87','country','Montenegro','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('88','country','Saint Kitts and Nevis','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('89','country','Togo','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('90','country','China','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('91','country','Armenia','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('92','country','Antigua and Barbuda','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('93','country','Dominican Republic','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('94','country','Ukraine','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('95','country','Bahrain','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('96','country','Tonga','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('97','country','Finland','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('98','country','Libya','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('99','country','Indonesia','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('100','country','Central African Republic','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('101','country','United States','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('102','country','Sweden','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('103','country','Vietnam','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('104','country','British Virgin Islands','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('105','country','Mali','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('106','country','Russia','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('107','country','Bulgaria','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('108','country','Mauritius','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('109','country','Romania','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('110','country','Angola','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('111','country','Portugal','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('112','country','South Africa','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('113','country','Fiji','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('114','country','Brunei Darussalam','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('115','country','Malaysia','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('116','country','Austria','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('117','country','Mozambique','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('118','country','Uganda','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('119','country','Kyrgyz Republic','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('120','country','Hungary','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('121','country','Niger','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('122','country','Brazil','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('123','country','Falkland Islands','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('124','country','Kuwait','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('125','country','Panama','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('126','country','Guyana','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('127','country','Costa Rica','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('128','country','Luxembourg','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('129','country','American Samoa','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('130','country','Gibraltar','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('131','country','Ireland','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('132','country','Nigeria','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('133','country','Ecuador','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('134','country','Czech Republic','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('135','country','Australia','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('136','country','Iran','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('137','country','Algeria','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('138','country','El Salvador','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('139','country','Saint Pierre and Miquelon','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('140','country','Chile','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('141','country','Puerto Rico','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('142','country','Belgium','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('143','country','Thailand','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('144','country','Haiti','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('145','country','Iraq','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('146','country','Sierra Leone','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('147','country','Georgia','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('148','country','Hong Kong (China)','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('149','country','Denmark','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('150','country','Poland','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('151','country','Moldova','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('152','country','Morocco','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('153','country','Croatia','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('154','country','Mongolia','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('155','country','Guinea-Bissau','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('156','country','Kiribati','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('157','country','Switzerland','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('158','country','Grenada','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('159','country','Belize','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('160','country','Chad','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('161','country','Estonia','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('162','country','Uruguay','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('163','country','Equatorial Guinea','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('164','country','Lebanon','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('165','country','Uzbekistan','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('166','country','Tunisia','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('167','country','Djibouti','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('168','country','Rwanda','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('169','country','Timor-Leste','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('170','country','Dominica','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('171','country','Colombia','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('172','country','Reunion','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('173','country','Burundi','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('174','country','Taiwan','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('175','country','Nicaragua','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('176','country','Barbados','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('177','country','Qatar','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('178','country','Congo, Dem. Rep.','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('179','country','Italy','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('180','country','Bhutan','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('181','country','Sudan','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('182','country','Nepal','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('183','country','Malta','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('184','country','Maldives','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('185','country','Suriname','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('186','country','Cayman Islands','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('187','country','Venezuela','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('188','country','Israel','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('189','country','Iceland','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('190','country','Zambia','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('191','country','Senegal','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('192','country','Papua New Guinea','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('193','country','Malawi','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('194','country','Macau (China)','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('195','country','Trinidad and Tobago','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('196','country','Zimbabwe','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('197','country','Germany','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('198','country','Vanuatu','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('199','country','Martinique','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('200','country','Kazakhstan','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('201','country','Philippines','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('202','country','Eritrea','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('203','country','Montserrat','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('204','country','New Caledonia','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('205','country','Macedonia','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('206','country','North Korea','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('207','country','Paraguay','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('208','country','Latvia','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('209','country','Japan','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('210','country','Syria','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('211','country','Honduras','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('212','country','Myanmar','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('213','country','Mexico','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('214','country','Egypt','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('215','country','Singapore','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('216','country','Serbia','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('217','country','Botswana','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('218','country','United Kingdom','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('219','country','Antarctica','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('220','country','Gambia, The','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('221','country','Greece','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('222','country','Sri Lanka','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('223','country','French Guiana','World');
insert into iamc_nodes(id, hierarchy, name, parent) values ('224','country','Comoros','World');
