-- Cloud Spanner DDL Schema for LEI Entities and Locations with Graph Support and S2 Geo-Spatial Indexing
-- Based on Spanner Property Graph (GQL) and S2 multi-level spatial indexing pattern.

CREATE TABLE Entities (
    LEI STRING(20) NOT NULL,
    LegalName STRING(MAX),
    LegalJurisdiction STRING(100),
    EntityCategory STRING(100),
    EntityStatus STRING(50),
    EntityCreationDate TIMESTAMP,
    InitialRegistrationDate TIMESTAMP,
    LastUpdateDate TIMESTAMP,
    RegistrationStatus STRING(50),
    NextRenewalDate TIMESTAMP,
    ManagingLOU STRING(50),
    ValidationSources STRING(100),
    ValidationAuthorityID STRING(50),
    ValidationAuthorityEntityID STRING(50),
    ConformityFlag STRING(50),
    EntityLegalFormCode STRING(50),
    OtherLegalForm STRING(255),
    RawData JSON
) PRIMARY KEY (LEI);

CREATE TABLE EntityLocations (
    LocationId STRING(64) NOT NULL,
    LEI STRING(20) NOT NULL,
    AddressType STRING(50) NOT NULL, -- 'LEGAL' or 'HEADQUARTERS'
    FirstAddressLine STRING(MAX),
    AdditionalAddressLine STRING(MAX),
    City STRING(255),
    Region STRING(100),
    Country STRING(100),
    PostalCode STRING(50),
    Latitude FLOAT64,
    Longitude FLOAT64,
    S2CellId INT64 NOT NULL,
    S2TokenStr STRING(32) NOT NULL,
    FOREIGN KEY (LEI) REFERENCES Entities (LEI)
) PRIMARY KEY (LocationId);

CREATE TABLE LocationS2Tokens (
    LocationId STRING(64) NOT NULL,
    S2Level INT64 NOT NULL,
    S2Token INT64 NOT NULL,
    S2TokenStr STRING(32) NOT NULL
) PRIMARY KEY (LocationId, S2Level, S2Token),
  INTERLEAVE IN PARENT EntityLocations ON DELETE CASCADE;

CREATE TABLE EntityHasLocation (
    LEI STRING(20) NOT NULL,
    LocationId STRING(64) NOT NULL,
    RelationshipType STRING(50) NOT NULL,
    CreatedAt TIMESTAMP,
    FOREIGN KEY (LEI) REFERENCES Entities (LEI),
    FOREIGN KEY (LocationId) REFERENCES EntityLocations (LocationId)
) PRIMARY KEY (LEI, LocationId);

-- Secondary index for fast range and point lookups on S2 tokens across levels
CREATE INDEX IndexLocationS2TokensByToken ON LocationS2Tokens(S2Token, S2Level);

-- Secondary index on exact location leaf cell IDs
CREATE INDEX IndexEntityLocationsByS2CellId ON EntityLocations(S2CellId);

-- Property Graph Definition incorporating Entity and Location nodes with HAS_LOCATION relationship
CREATE PROPERTY GRAPH LEIGraph
  NODE TABLES (
    Entities
      KEY (LEI)
      LABEL Entity
      PROPERTIES (
        LEI,
        LegalName,
        LegalJurisdiction,
        EntityCategory,
        EntityStatus,
        EntityCreationDate,
        InitialRegistrationDate,
        LastUpdateDate,
        RegistrationStatus
      ),
    EntityLocations
      KEY (LocationId)
      LABEL Location
      PROPERTIES (
        LocationId,
        LEI,
        AddressType,
        FirstAddressLine,
        AdditionalAddressLine,
        City,
        Region,
        Country,
        PostalCode,
        Latitude,
        Longitude,
        S2CellId,
        S2TokenStr
      )
  )
  EDGE TABLES (
    EntityHasLocation
      KEY (LEI, LocationId)
      SOURCE KEY (LEI) REFERENCES Entities (LEI)
      DESTINATION KEY (LocationId) REFERENCES EntityLocations (LocationId)
      LABEL HAS_LOCATION
      PROPERTIES (RelationshipType, CreatedAt)
  );
