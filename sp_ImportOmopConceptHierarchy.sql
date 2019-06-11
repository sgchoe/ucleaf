USE [LeafDB]
GO
/******
-- Author:		Seong Choi
-- Modified:	2019-06-10
-- Description:	Import OMOP concepts from 'LeafClinDB' database into 'LeafDB' app.Concept table.
--				OMOP concept, concept_relationship, and concept_ancestor tables must exist and be
--				fully populated as the first two tables will be used to build the concept hierarchy
--				and the latter will be referenced in each Leaf concept's 'SqlSetWhere' clause.  The
--				final update to link parent/child concepts in the Leaf concept table may take a long
--				time to complete if the ExternalId and ExternalParentId columns aren't indexed, even
--				when importing a relatively small concept hierarchy of ~10k records.  The Leaf db
--				must be in 'Simple' recovery mode for proper batched execution that minimizes log
--				space usage.  Batch size defaults to 100k if not specified.  OMOP concepts that are
--				imported can be restricted by providing a comma-separated list of domain_ids, e.g.
--				'Condition,Drug,Medication,Procedure'.
******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[sp_ImportOmopConceptHierarchy]
	@omopRootConceptId INT,
	@leafRootConceptId UNIQUEIDENTIFIER,
	@batchSize INT = 100000,
	@omopAllowedConceptDomainIds VARCHAR(50) = ''
AS
BEGIN
	SET NOCOUNT ON;

	IF (EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES  WHERE TABLE_SCHEMA = 'dbo' AND  TABLE_NAME = '__omopConcepts'))
		DROP TABLE dbo.__omopConcepts

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

	DECLARE @domainIdWildcards VARCHAR = '%, *, ';
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
			(ExternalId,
			ExternalParentId,
			IsParent,
			SqlSetId,
			SqlSetWhere,
			UiDisplayName,
			UiDisplayText,
			AddDateTime,
			ContentLastUpdateDateTime)
		SELECT
			'OMOP:' + CONVERT(VARCHAR, _oc.child_concept_id) + ':' + _oc.child_concept_code,
			'OMOP:' + CONVERT(VARCHAR, _oc.parent_concept_id) + ':' + _oc.parent_concept_code,
			CASE
				WHEN EXISTS (SELECT 1 FROM dbo.__omopConcepts opchp WHERE opchp.parent_concept_id = _oc.child_concept_id) THEN
					1
				ELSE
					0
			END,
			@leafRootConceptSqlSetId,
			'EXISTS (SELECT 1 FROM concept_ancestor ca WHERE ca.descendant_concept_id = @.concept_id ' +
				'AND ca.ancestor_concept_id = ' + CONVERT(varchar, _oc.child_concept_id) + ')',
			CASE
				WHEN _oc.child_concept_name LIKE '%[0-9A-Za-z]%' THEN
					_oc.child_concept_name
				ELSE
					_oc.child_concept_code
			END, -- use OMOP concept code instead of name if blank, "" (CPT), or is otherwise not meaningful
			'Had observation: ' +
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
