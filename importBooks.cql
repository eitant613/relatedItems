WITH {fileName} AS url
CALL apoc.load.json(url) YIELD value
UNWIND value.docs AS row

MERGE (n:Book2{id:row.id}) ON CREATE
  SET
    n.PQID=row.PQID,
    n.SSID=row.SSID,
    n.EISSN=row.EISSN,
    n.ISSN=row.ISSN,
    n.EIBSN=row.EIBSN,
    n.ISBN=row.ISBN,
    n.ORCID=row.ORCID,
    n.DOI=row.DOI,
    n.Abstract_t=row.Abstract_t,
    n.TitleHash_sh=row.TitleHash_sh,
    n.TitleSubtitleHash_sh=row.TitleSubtitleHash_sh,
    n.Title_t=row.Title_t,
    n.Author_t=row.Author_t,
    n.Subtitle_t=row.Subtitle_t,
    n.sys_external_document_id =row.sys_external_document_id,
    n.sys_source_id_package_id=row.sys_source_id_package_id,
    n.metadataParticipantMergeId=row.metadataParticipantMergeId,
    n.sys_participant_id=row.sys_participant_id,
    n.sys_content_type =row.sys_content_type
