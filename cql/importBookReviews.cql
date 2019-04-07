WITH {fileName} AS url
CALL apoc.load.json(url) YIELD value
UNWIND value.docs AS row

MERGE (n:BookReview{id:row.id}) ON CREATE
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
    n.StartPage_t=row.StartPage_t,
    n.issue_t=row.Issue_t,
    n.PublicationTitle_t=row.PublicationTitle_t,
    n.volume_t=row.rowVolume_t,
    n.PublicationDateYear_s=row.PublicationDateYear_s,
    n.norm_title1=row.Norm_Title1,
    n.norm_title2=row.Norm_Title2,
    n.orig_title=row.Orig_Title,
    n.orig_title_length=length(row.Orig_Title),
    n.norm_author1=row.Norm_Author1,
    n.norm_author2=row.Norm_Author2
