WITH {fileName} AS url
CALL apoc.load.json(url) YIELD value
UNWIND value.docs AS row

MERGE (n:Book{id:row.id}) ON CREATE
  SET
    n.pqid=row.PQID,
    n.ssid=row.SSID,
    n.eissn=row.EISSN,
    n.issn=row.ISSN,
    n.eisbn=row.EISBN,
    n.isbn=row.ISBN,
    n.orcid=row.ORCID,
    n.doi=row.DOI,
    n.dbid=row.DBID,
    n.abstract_t=row.Abstract_t,
    n.TitleHash_sh=row.TitleHash_sh,
    n.TitleSubtitleHash_sh=row.TitleSubtitleHash_sh,
    n.title_t=row.Title_t,
    n.author_t=row.Author_t,
    n.subtitle_t=row.Subtitle_t,
    n.sys_external_document_id =row.sys_external_document_id,
    n.sys_source_id_package_id=row.sys_source_id_package_id,
    n.metadataParticipantMergeId=row.metadataParticipantMergeId,
    n.sys_participant_id=row.sys_participant_id,
    n.sys_content_type =row.sys_content_type,
    n.norm_title=row.Norm_Title,
	n.norm_title_length=row.Norm_Title_Length,
    n.orig_title=row.Orig_Title,
	n.orig_title_length=row.Orig_Title_Length,
    n.norm_author=row.Norm_Author





