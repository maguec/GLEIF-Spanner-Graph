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
    ManagingLOU STRING(100),
    ValidationSources STRING(255),
    ValidationAuthorityID STRING(100),
    ValidationAuthorityEntityID STRING(255),
    ConformityFlag STRING(50),
    EntityLegalFormCode STRING(100),
    OtherLegalForm STRING(MAX),
    RawData JSON,
    lc STRING(MAX) AS (LOWER(LegalName)) HIDDEN,
    name_Tokens TOKENLIST AS (TOKENIZE_NGRAMS(lc, ngram_size_min=>2, ngram_size_max=>4)) HIDDEN,
    name_FullText TOKENLIST AS (TOKENIZE_FULLTEXT(lc)) HIDDEN,
    name_SubString TOKENLIST AS (TOKENIZE_SUBSTRING(lc)) HIDDEN
) PRIMARY KEY (LEI);

CREATE TABLE EntityLocations (
    LocationId STRING(64) NOT NULL,
    LEI STRING(20) NOT NULL,
    AddressType STRING(50) NOT NULL, -- 'LEGAL' or 'HEADQUARTERS'
    FirstAddressLine STRING(MAX),
    AdditionalAddressLine STRING(MAX),
    City STRING(255),
    Region STRING(255),
    Country STRING(255),
    PostalCode STRING(100),
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

-- Interleaved relationship table between entities (e.g. parent/subsidiary, fund manager, subfund)
CREATE TABLE EntityRelationships (
    LEI STRING(20) NOT NULL,
    EndLEI STRING(20) NOT NULL,
    RelationshipType STRING(100) NOT NULL,
    RelationshipStatus STRING(50),
    InitialRegistrationDate TIMESTAMP,
    LastUpdateDate TIMESTAMP,
    RegistrationStatus STRING(50),
    NextRenewalDate TIMESTAMP,
    ManagingLOU STRING(100),
    ValidationSources STRING(255),
    FOREIGN KEY (LEI) REFERENCES Entities (LEI),
    FOREIGN KEY (EndLEI) REFERENCES Entities (LEI)
) PRIMARY KEY (LEI, EndLEI, RelationshipType),
  INTERLEAVE IN PARENT Entities ON DELETE CASCADE;

-- Analytics table storing PageRank scores, Community IDs, and Jaccard similarity metrics
CREATE TABLE EntityGraphAnalytics (
    LEI STRING(20) NOT NULL,
    PageRankScore FLOAT64,
    CommunityId INT64,
    JaccardCommunityId INT64,
    JaccardSimilarityScore FLOAT64,
    LastUpdated TIMESTAMP DEFAULT (CURRENT_TIMESTAMP())
) PRIMARY KEY (LEI),
  INTERLEAVE IN PARENT Entities ON DELETE CASCADE;

-- Secondary index for fast range and point lookups on S2 tokens across levels
CREATE INDEX IndexLocationS2TokensByToken ON LocationS2Tokens(S2Token, S2Level);

-- Secondary index on exact location leaf cell IDs
CREATE INDEX IndexEntityLocationsByS2CellId ON EntityLocations(S2CellId);

-- Secondary index for fast reverse lookups on EndLEI (e.g. finding parent, manager, or master fund)
CREATE INDEX IndexEntityRelationshipsByEndLEI ON EntityRelationships(EndLEI, LEI);

-- Secondary indexes for fast PageRank ranking and Community cluster lookups
CREATE INDEX IndexEntityGraphAnalyticsByPageRank ON EntityGraphAnalytics(PageRankScore DESC);
CREATE INDEX IndexEntityGraphAnalyticsByCommunity ON EntityGraphAnalytics(CommunityId, PageRankScore DESC);

-- Full-text, N-Gram, and Substring Search Index on Entity Legal Names
CREATE SEARCH INDEX EntitiesNameSearchIndex ON Entities(name_Tokens, name_FullText, name_SubString);

-- Property Graph Definition incorporating Entity and Location nodes with HAS_LOCATION and IS_RELATED_TO relationships
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
      PROPERTIES (RelationshipType, CreatedAt),
    EntityRelationships
      KEY (LEI, EndLEI, RelationshipType)
      SOURCE KEY (LEI) REFERENCES Entities (LEI)
      DESTINATION KEY (EndLEI) REFERENCES Entities (LEI)
      LABEL IS_RELATED_TO
      PROPERTIES (
        RelationshipType,
        RelationshipStatus,
        InitialRegistrationDate,
        LastUpdateDate,
        RegistrationStatus,
        ManagingLOU
      )
  );
