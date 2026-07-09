use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    Aiya,
    Guardian,
    Judge,
    Citizen,
}

impl Role {
    pub fn as_str(&self) -> &'static str {
        match self {
            Role::Aiya => "Aiya",
            Role::Guardian => "Guardian",
            Role::Judge => "Judge",
            Role::Citizen => "Citizen",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "Aiya" => Some(Role::Aiya),
            "Guardian" => Some(Role::Guardian),
            "Judge" => Some(Role::Judge),
            "Citizen" => Some(Role::Citizen),
            _ => None,
        }
    }

    pub fn satisfies(&self, required: &Role) -> bool {
        match required {
            Role::Aiya => matches!(self, Role::Aiya),
            Role::Guardian => matches!(self, Role::Aiya | Role::Guardian),
            Role::Judge => matches!(self, Role::Aiya | Role::Guardian | Role::Judge),
            Role::Citizen => true,
        }
    }

    /// Governance roles (Guardian, Judge, Aiya) are assigned only via candidacy appointment.
    pub fn is_governance(&self) -> bool {
        matches!(self, Role::Aiya | Role::Guardian | Role::Judge)
    }
}

pub fn is_governance_role_str(role: &str) -> bool {
    matches!(role, "Aiya" | "Guardian" | "Judge")
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum QuazarEventType {
    SystemInit,
    SystemUpgrade,
    SystemConfig,
    CitizenRequested,
    CitizenAdded,
    CitizenRemoved,
    CitizenUpdated,
    CitizenSuspended,
    CitizenRestored,
    PassportIssued,
    PassportSuspended,
    PassportRevoked,
    LawProposed,
    LawAmended,
    LawRepealed,
    LawAdded,
    LawVoteStarted,
    LawVoteResult,
    ElectionAnnounced,
    ElectionCandidate,
    ElectionVoteStarted,
    ElectionVoteResult,
    AiyaElected,
    AppointmentGuardian,
    AppointmentJudge,
    AppointmentRevoked,
    CourtCaseOpened,
    CourtRuling,
    CourtAppeal,
    CourtAppealRuling,
    DomainRegistered,
    DomainTransferred,
    DomainExpired,
    NodeAdded,
    NodeRemoved,
    InfraMigration,
    ConstitutionFullText,
    VoteStarted,
    VoteCast,
    VoteFinalized,
    PeerListUpdate,
    CandidateNominated,
    CandidateVoted,
    CandidateApproved,
    CandidateAppointed,
}

impl ToString for QuazarEventType {
    fn to_string(&self) -> String {
        format!("{:?}", self)
    }
}

impl QuazarEventType {
    pub fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "SystemInit" => Ok(QuazarEventType::SystemInit),
            "SystemUpgrade" => Ok(QuazarEventType::SystemUpgrade),
            "SystemConfig" => Ok(QuazarEventType::SystemConfig),
            "CitizenRequested" => Ok(QuazarEventType::CitizenRequested),
            "CitizenAdded" => Ok(QuazarEventType::CitizenAdded),
            "CitizenRemoved" => Ok(QuazarEventType::CitizenRemoved),
            "CitizenUpdated" => Ok(QuazarEventType::CitizenUpdated),
            "CitizenSuspended" => Ok(QuazarEventType::CitizenSuspended),
            "CitizenRestored" => Ok(QuazarEventType::CitizenRestored),
            "PassportIssued" => Ok(QuazarEventType::PassportIssued),
            "PassportSuspended" => Ok(QuazarEventType::PassportSuspended),
            "PassportRevoked" => Ok(QuazarEventType::PassportRevoked),
            "LawProposed" => Ok(QuazarEventType::LawProposed),
            "LawAmended" => Ok(QuazarEventType::LawAmended),
            "LawRepealed" => Ok(QuazarEventType::LawRepealed),
            "LawAdded" => Ok(QuazarEventType::LawAdded),
            "LawVoteStarted" => Ok(QuazarEventType::LawVoteStarted),
            "LawVoteResult" => Ok(QuazarEventType::LawVoteResult),
            "ElectionAnnounced" => Ok(QuazarEventType::ElectionAnnounced),
            "ElectionCandidate" => Ok(QuazarEventType::ElectionCandidate),
            "ElectionVoteStarted" => Ok(QuazarEventType::ElectionVoteStarted),
            "ElectionVoteResult" => Ok(QuazarEventType::ElectionVoteResult),
            "AiyaElected" => Ok(QuazarEventType::AiyaElected),
            "AppointmentGuardian" => Ok(QuazarEventType::AppointmentGuardian),
            "AppointmentJudge" => Ok(QuazarEventType::AppointmentJudge),
            "AppointmentRevoked" => Ok(QuazarEventType::AppointmentRevoked),
            "CourtCaseOpened" => Ok(QuazarEventType::CourtCaseOpened),
            "CourtRuling" => Ok(QuazarEventType::CourtRuling),
            "CourtAppeal" => Ok(QuazarEventType::CourtAppeal),
            "CourtAppealRuling" => Ok(QuazarEventType::CourtAppealRuling),
            "DomainRegistered" => Ok(QuazarEventType::DomainRegistered),
            "DomainTransferred" => Ok(QuazarEventType::DomainTransferred),
            "DomainExpired" => Ok(QuazarEventType::DomainExpired),
            "NodeAdded" => Ok(QuazarEventType::NodeAdded),
            "NodeRemoved" => Ok(QuazarEventType::NodeRemoved),
            "InfraMigration" => Ok(QuazarEventType::InfraMigration),
            "ConstitutionFullText" => Ok(QuazarEventType::ConstitutionFullText),
            "VoteStarted" => Ok(QuazarEventType::VoteStarted),
            "VoteCast" => Ok(QuazarEventType::VoteCast),
            "VoteFinalized" => Ok(QuazarEventType::VoteFinalized),
            "PeerListUpdate" => Ok(QuazarEventType::PeerListUpdate),
            "CandidateNominated" => Ok(QuazarEventType::CandidateNominated),
            "CandidateVoted" => Ok(QuazarEventType::CandidateVoted),
            "CandidateApproved" => Ok(QuazarEventType::CandidateApproved),
            "CandidateAppointed" => Ok(QuazarEventType::CandidateAppointed),
            _ => Err(format!("Unknown event type: {}", s)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Role;

    #[test]
    fn role_from_str_and_as_str_roundtrip() {
        for role in [Role::Aiya, Role::Guardian, Role::Judge, Role::Citizen] {
            assert_eq!(Role::from_str(role.as_str()), Some(role.clone()));
        }
    }

    #[test]
    fn role_satisfies_hierarchy() {
        assert!(Role::Aiya.satisfies(&Role::Guardian));
        assert!(Role::Guardian.satisfies(&Role::Judge));
        assert!(!Role::Citizen.satisfies(&Role::Guardian));
        assert!(Role::Citizen.satisfies(&Role::Citizen));
    }

    #[test]
    fn governance_role_detection() {
        assert!(Role::Guardian.is_governance());
        assert!(Role::Judge.is_governance());
        assert!(Role::Aiya.is_governance());
        assert!(!Role::Citizen.is_governance());
        assert!(super::is_governance_role_str("Guardian"));
        assert!(!super::is_governance_role_str("Citizen"));
    }
}
