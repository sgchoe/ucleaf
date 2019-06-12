USE [LeafDB]
GO
/****** Object:  StoredProcedure [dbo].[sp_ImportOmopConceptHierarchy]    Script Date: 6/12/2019 11:06:56 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [dbo].[sp_ImportOmopConceptHierarchy]
	@omopRootConceptId INT,
	@omopConceptIdColumnName VARCHAR(255),
	@leafRootConceptId UNIQUEIDENTIFIER,
	@leafDisplayTextPrefix VARCHAR(255),
	@batchSize INT = 100000,
	@omopAllowedConceptDomainIds VARCHAR(255) = ''
AS
BEGIN
	SET NOCOUNT ON;

	IF (EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES  WHERE TABLE_SCHEMA = 'dbo' AND  TABLE_NAME = '__omopConcepts'))
		DROP TABLE dbo.__omopConcepts;

	IF (NOT EXISTS (SELECT 1 FROM LeafClinDB.dbo.concept WHERE concept_id = @omopRootConceptId))
		THROW 50000, 'OMOP root concept not found', 1;

	IF (NOT EXISTS (SELECT 1 FROM app.Concept WHERE Id = @leafRootConceptId AND RootId IS NOT NULL))
		THROW 50000, 'Leaf root concept not found or invalid (non-null RootId required)', 1;

	CREATE TABLE dbo.__omopConcepts
	(
		row_id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
		parent_concept_id INT not null,
		parent_concept_name VARCHAR(255) not null,
		parent_concept_code VARCHAR(50) not null,
		child_concept_id INT not null,
		child_concept_name VARCHAR(255) not null,
		child_concept_code VARCHAR(50) not null
	);

	CREATE INDEX IX___OC_CONCEPT_ID ON dbo.__omopConcepts (parent_concept_id);

	DECLARE @domainIdWildcards VARCHAR(50) = '%, *, ';
	-- Recursively find all parent/child relationship permutations under specified OMOP root concept
	WITH omopParentChildConcepts AS
	(
		SELECT
			parent.concept_id AS parent_concept_id,
			parent.concept_name AS parent_concept_name,
			parent.concept_code AS parent_concept_code,
			child.concept_id AS child_concept_id,
			child.concept_name AS child_concept_name,
			child.concept_code AS child_concept_code
		FROM
			LeafClinDB.dbo.concept_relationship cr
		JOIN
			LeafClinDB.dbo.concept parent ON parent.concept_id = cr.concept_id_1
		JOIN
			LeafClinDB.dbo.concept child ON child.concept_id = cr.concept_id_2
		WHERE
			cr.concept_id_1 = @omopRootConceptId AND
			cr.relationship_id = 'Subsumes' AND
			(
				ISNULL(@omopAllowedConceptDomainIds, '') IN (SELECT TRIM(value) FROM STRING_SPLIT(@domainIdWildcards, ',')) OR
				child.domain_id IN (SELECT TRIM(value) FROM STRING_SPLIT(@omopAllowedConceptDomainIds, ','))
			)
		UNION ALL
		SELECT
			parent.concept_id AS parent_concept_id,
			parent.concept_name AS parent_concept_name,
			parent.concept_code AS parent_concept_code,
			child.concept_id AS child_concept_id,
			child.concept_name AS child_concept_name,
			child.concept_code AS child_concept_code
		FROM
			LeafClinDB.dbo.concept_relationship cr
		JOIN
			omopParentChildConcepts opcc ON opcc.child_concept_id = cr.concept_id_1 AND cr.relationship_id = 'Subsumes'
		JOIN
			LeafClinDB.dbo.concept parent ON parent.concept_id = cr.concept_id_1
		JOIN
			LeafClinDB.dbo.concept child ON child.concept_id = cr.concept_id_2
		WHERE
			(
				ISNULL(@omopAllowedConceptDomainIds, '') IN (SELECT TRIM(value) FROM STRING_SPLIT(@domainIdWildcards, ',')) OR
				child.domain_id IN (SELECT TRIM(value) FROM STRING_SPLIT(@omopAllowedConceptDomainIds, ','))
			)
	)

	INSERT INTO
		dbo.__omopConcepts
		(parent_concept_id, parent_concept_name, parent_concept_code, child_concept_id, child_concept_name, child_concept_code)
	SELECT DISTINCT
		parent_concept_id, parent_concept_name, parent_concept_code, child_concept_id, child_concept_name, child_concept_code
	FROM
		omopParentChildConcepts
	ORDER BY
		parent_concept_name, parent_concept_id, child_concept_name, child_concept_id;

	PRINT CONVERT(VARCHAR, @@ROWCOUNT) + ' rows inserted into OMOP hierarchy cache table';

	IF NOT EXISTS (SELECT 1 FROM dbo.__omopConcepts)
	BEGIN
		PRINT 'Exiting';
		RETURN;
	END

	CHECKPOINT;

	DECLARE @currentDateTime DATETIME = GETDATE();
	DECLARE @omopRootConceptCode VARCHAR(50);
	DECLARE @leafRootConceptRootId UNIQUEIDENTIFIER;
	DECLARE @leafRootConceptSqlSetId INT;
	DECLARE @lastProcessedRowId INT = 0;
	DECLARE @maxRowId INT = 0;

	SELECT @omopRootConceptCode = concept_code FROM LeafClinDB.dbo.concept WHERE concept_id = @omopRootConceptId;
	
	SELECT
		@leafRootConceptRootId = RootId,
		@leafRootConceptSqlSetId = SqlSetId
	FROM
		app.Concept
	WHERE
		Id = @leafRootConceptId;

	SELECT @maxRowId = MAX(row_id) FROM dbo.__omopConcepts;

	-- Set specified Leaf root concept's external id so that parent/child linkage can be established
	UPDATE
		app.Concept
	SET
		ExternalId = 'OMOP:' + CONVERT(VARCHAR, @omopRootConceptId) + ':' + @omopRootConceptCode,
		IsParent = 1
	WHERE
		Id = @leafRootConceptId;

	-- Batch insert Leaf concepts for all OMOP parent/child pairs
	WHILE @lastProcessedRowId <= @maxRowId
	BEGIN
		INSERT INTO app.Concept
			(RootId,
			ExternalId,
			ExternalParentId,
			IsParent,
			SqlSetId,
			SqlSetWhere,
			UiDisplayName,
			UiDisplayText,
			AddDateTime,
			ContentLastUpdateDateTime)
		SELECT
			@leafRootConceptRootId,
			'OMOP:' + CONVERT(VARCHAR, _oc.child_concept_id) + ':' + _oc.child_concept_code,
			'OMOP:' + CONVERT(VARCHAR, _oc.parent_concept_id) + ':' + _oc.parent_concept_code,
			CASE
				WHEN EXISTS (SELECT 1 FROM dbo.__omopConcepts opchp WHERE opchp.parent_concept_id = _oc.child_concept_id) THEN
					1
				ELSE
					0
			END,
			@leafRootConceptSqlSetId,
			'EXISTS (SELECT 1 FROM concept_ancestor ca WHERE ca.descendant_concept_id = @.' + @omopConceptIdColumnName + ' ' +
				'AND ca.ancestor_concept_id = ' + CONVERT(varchar, _oc.child_concept_id) + ')',
			CASE
				WHEN _oc.child_concept_name LIKE '%[0-9A-Za-z]%' THEN
					_oc.child_concept_name
				ELSE
					_oc.child_concept_code
			END, -- use OMOP concept code instead of name if blank, "" (CPT), or is otherwise not meaningful
			@leafDisplayTextPrefix +
				CASE
					WHEN _oc.child_concept_name LIKE '%[0-9A-Za-z]%' THEN
						_oc.child_concept_name
					ELSE
						_oc.child_concept_code
				END, -- use OMOP concept code instead of name if blank, "" (CPT), or is otherwise not meaningful
			@currentDateTime,
			@currentDateTime
		FROM
			dbo.__omopConcepts _oc
		WHERE
			_oc.row_id BETWEEN @lastProcessedRowId AND @lastProcessedRowId + @batchSize;

		CHECKPOINT;

		SELECT @lastProcessedRowId = @lastProcessedRowId + @batchSize + 1;

		PRINT CONVERT(VARCHAR, CASE WHEN @lastProcessedRowId > @maxRowId THEN @maxRowId ELSE @lastProcessedRowId END) +
			' Leaf concepts added';
	END

	DROP TABLE dbo.__omopConcepts;
	
	-- Establish parent/child linkage of newly inserted Leaf concepts
	DECLARE @rowCount INT = 1;
	WHILE @rowCount > 0
	BEGIN
		UPDATE TOP (@batchSize)
			app.Concept
		SET
			Concept.ParentId = parent.Id
		FROM
			(SELECT ExternalId, ExternalParentId FROM app.Concept) child
		JOIN
			(SELECT Id, ExternalId FROM app.Concept) parent ON child.ExternalParentId = parent.ExternalId
		WHERE
			Concept.ParentId IS NULL
			AND Concept.AddDateTime = @currentDateTime
			AND Concept.ExternalId = child.ExternalId
			AND Concept.ExternalParentId = child.ExternalParentId;

		SELECT @rowCount = @@ROWCOUNT;

		IF @rowCount > 0
			PRINT CONVERT(VARCHAR, @rowCount) + ' Leaf concept parent linkages updated';

		CHECKPOINT;
	END
END
