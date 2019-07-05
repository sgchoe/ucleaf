USE [LeafDB]
GO
/****** Object:  StoredProcedure [dbo].[sp_DeleteConceptChildren]    Script Date: 7/5/2019 1:32:13 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [dbo].[sp_DeleteConceptChildren]
	@ConceptId UNIQUEIDENTIFIER
AS

CREATE TABLE #conceptIdsToDelete
(
	Id UNIQUEIDENTIFIER NOT NULL,
	PRIMARY KEY (Id)
);

PRINT 'Finding Concept Ids to delete';

;WITH parentChildConcepts AS
(
	SELECT Id, ParentId, RootId
	FROM app.Concept
	WHERE ParentId = @ConceptId
	UNION ALL
	SELECT c.Id, c.ParentId, c.RootId
	FROM app.Concept c
	JOIN parentChildConcepts pcc ON pcc.Id = c.ParentId
)
INSERT INTO #conceptIdsToDelete SELECT DISTINCT Id FROM parentChildConcepts ORDER BY Id DESC;

DECLARE @conceptCount INT;
SELECT @conceptCount = COUNT(Id) FROM #conceptIdsToDelete;

PRINT 'Found ' + CAST(@conceptCount AS VARCHAR) + ' child concepts to delete';

BEGIN TRANSACTION;
	PRINT 'Deleting from app.PanelFilter';
	DELETE FROM app.PanelFilter WHERE EXISTS (SELECT 1 FROM #conceptIdsToDelete citd WHERE citd.Id = ConceptId);

	PRINT 'Deleting from rela.QueryConceptDependency';
	DELETE FROM rela.QueryConceptDependency WHERE EXISTS (SELECT 1 FROM #conceptIdsToDelete citd WHERE citd.Id = DependsOn);

	PRINT 'Deleting from auth.ConceptConstraint';
	DELETE FROM auth.ConceptConstraint WHERE EXISTS (SELECT 1 FROM #conceptIdsToDelete citd WHERE citd.Id = ConceptId);

	PRINT 'Deleting from rela.ConceptSpecializationGroup';
	DELETE FROM rela.ConceptSpecializationGroup WHERE EXISTS (SELECT 1 FROM #conceptIdsToDelete citd WHERE citd.Id = ConceptId);

	PRINT 'Deleting from app.ConceptTokenizedIndex';
	DELETE FROM app.ConceptTokenizedIndex WHERE EXISTS (SELECT 1 FROM #conceptIdsToDelete citd WHERE citd.Id = ConceptId);

	PRINT 'Deleting from app.ConceptForwardIndex (ConceptId)';
	DELETE FROM app.ConceptForwardIndex WHERE EXISTS (SELECT 1 FROM #conceptIdsToDelete citd WHERE citd.Id = ConceptId);

	PRINT 'Deleting from app.ConceptForwardIndex (RootId)';
	DELETE FROM app.ConceptForwardIndex WHERE EXISTS (SELECT 1 FROM #conceptIdsToDelete citd WHERE citd.Id = RootId);

	PRINT 'Deleting ' + CAST(@conceptCount AS VARCHAR) + ' concepts';
	DELETE c FROM app.Concept c JOIN #conceptIdsToDelete citd ON citd.Id = c.Id;
COMMIT TRANSACTION;
