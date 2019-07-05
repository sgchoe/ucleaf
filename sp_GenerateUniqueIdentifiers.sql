USE [LeafDB]
GO
/****** Object:  StoredProcedure [dbo].[sp_GenerateUniqueIdentifiers]    Script Date: 7/5/2019 1:34:30 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [dbo].[sp_GenerateUniqueIdentifiers]
	@count INT
AS
DECLARE @guids TABLE (id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWSEQUENTIALID());
DECLARE @i INT = 0;
WHILE @i < ISNULL(@count, 0)
BEGIN
	INSERT INTO @guids DEFAULT VALUES;
	SET @i = @i + 1;
END
SELECT id FROM @guids ORDER BY id;
