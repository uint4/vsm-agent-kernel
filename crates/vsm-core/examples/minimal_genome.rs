use vsm_core::{LeafOperationSpec, OrganizationalGenome, OrganizationalGenomePatch, ViableNode};

fn main() {
    let mut root = ViableNode::new_metasystem("root-controller");
    root.system_5.identity = "Autonomous coding organization root".to_string();

    let code_service = ViableNode::new_leaf(
        "primary-code-service",
        LeafOperationSpec::coding(),
    );

    let mut genome = OrganizationalGenome::new(root);
    let root_id = genome.root_node_id.clone();
    let code_service_id = genome
        .add_child(&root_id, code_service)
        .expect("add primary code service");

    let root_caps = genome.get_node(&root_id).unwrap().capabilities();
    let code_caps = genome.get_node(&code_service_id).unwrap().capabilities();

    assert!(!root_caps.can_write_code);
    assert!(root_caps.can_delegate);
    assert!(code_caps.can_write_code);
    assert!(!code_caps.can_delegate);

    OrganizationalGenomePatch::PromoteLeafToMetasystem {
        node_id: code_service_id.clone(),
        extracted_child_name: "generalist-coder".to_string(),
    }
    .apply(&mut genome)
    .expect("promote leaf into metasystem");

    let promoted_caps = genome.get_node(&code_service_id).unwrap().capabilities();
    assert!(!promoted_caps.can_write_code);
    assert!(promoted_caps.can_delegate);

    let child_id = genome
        .get_node(&code_service_id)
        .unwrap()
        .children
        .first()
        .expect("extracted child exists")
        .clone();
    let child_caps = genome.get_node(&child_id).unwrap().capabilities();
    assert!(child_caps.can_write_code);
    assert!(!child_caps.can_delegate);

    println!("genome: {}", genome.id);
    println!("root: {}", root_id);
    println!("primary code service metasystem: {}", code_service_id);
    println!("extracted coding leaf: {}", child_id);
}
