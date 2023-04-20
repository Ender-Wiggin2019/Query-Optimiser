package sjdb;

import java.security.KeyPair;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Oushuo Huang
 * **/
public class Optimiser implements PlanVisitor {
    private Estimator es = new Estimator();
    private Inspector inspector = new Inspector(); // debug only
    private Set<Attribute> attributeSet = new HashSet<>();
    private Set<Predicate> predicateSet = new HashSet<>();
    private Set<Scan> scanSet = new HashSet<>();
    private List<Attribute> finalAttributes = new ArrayList<>();
    private Set<Attribute> projectedAttributeSet = new HashSet<>();

    public void visit(Scan op) {
        scanSet.add(new Scan((NamedRelation) op.getRelation()));
    }

    public void visit(Project op) {
        attributeSet.addAll(op.getAttributes());
        if (finalAttributes.size() == 0) {
            finalAttributes = op.getAttributes();
        }
    }
    public void visit(Product op) {
    }

    public void visit(Join op) {

    }

    public void visit(Select op) {
        predicateSet.add(op.getPredicate());
        attributeSet.add(op.getPredicate().getLeftAttribute());
        if( !op.getPredicate().equalsValue() ) {
            attributeSet.add(op.getPredicate().getRightAttribute());
        }
    }

    public Optimiser(Catalogue catalogue) {
    }

    public Operator optimise(Operator plan) {
        plan.accept(this);
        Set<Predicate> predicateSetCopy = new HashSet<>(predicateSet).stream().filter(predicate -> !predicate.equalsValue()).collect(Collectors.toSet());

        List<Operator> list = moveDown(plan);


        projectedAttributeSet = getProjectedAttributeSet(new ArrayList<>(predicateSet), plan);
//        System.out.println("projectedAttributeSet: " + projectedAttributeSet);
//        System.out.println("op after moveDown:"+ list);
//        System.out.println("---------------------------------");
//
//        System.out.println("attributeSet: " + attributeSet);
//        System.out.println("finalAttributes: " + finalAttributes);
//        System.out.println("list: " + list);
//        System.out.println("predicateSetCopy: " + predicateSetCopy);
        Operator result = reorder(list, predicateSetCopy);

        System.out.println("reorder result: " + result);
        result.accept(es);
//        result.accept(inspector);
        System.out.println("reorder render: " + result.getOutput().render());
        return result;
    }

    // TODO: separate to 2 methods
    private List<Operator> moveDown(Operator plan) {
//        Set<Predicate> predicateSetCopy = new HashSet<>(predicateSet);
        List<Operator> result = new ArrayList<>();

        for (Scan scan : scanSet) {
//            System.out.println("scan before moveDown:"+ scan);

            Operator moveDownSelectOp = moveDownSelect(scan, predicateSet);
//            System.out.println("op after moveDownSelect:"+ moveDownSelectOp);

            // test only, move down select first:
            result.add(moveDownSelectOp);


            Operator moveDownProjectOp = moveDownProject(moveDownSelectOp, getProjectedAttributeSet(new ArrayList<>(predicateSet), plan));
//            result.add(moveDownProjectOp);
//            System.out.println("op after moveDownProject:"+ moveDownProjectOp);
//            System.out.println("---------------------------------");
        }

//        System.out.println("predicateSet: " + predicateSet);

        return result;
    }

    /**
     * @param op the operator to be optimised
     * @return the optimised operator, with updated `predicateSet`
     */
    private Operator moveDownSelect(Operator op, Set<Predicate> predicateSet) {
//        System.out.println("predicateSet: " + predicateSet);
        Operator result = op;
        List<Attribute> attributes = result.getOutput().getAttributes();

        Iterator<Predicate> iter = predicateSet.iterator();
//        System.out.println("attributes: " + attributes);
        while (iter.hasNext()) {
            Predicate predicate = iter.next();
//            System.out.println("predicate:" + predicate.toString());
//            System.out.println("predicate.getLeftAttribute():" + predicate.getLeftAttribute().toString());
//            System.out.println("attributes.contains(predicate.getLeftAttribute()):" + attributes.contains(predicate.getLeftAttribute()));
            if (result.getOutput() == null ) result.accept(es);

            // if attr=value or attr1=attr2, move down `Select` operator
            if ((predicate.equalsValue() && attributes.contains(predicate.getLeftAttribute())) ||
                    (!predicate.equalsValue() && attributes.contains(predicate.getLeftAttribute()) && attributes.contains(predicate.getRightAttribute()))) {
                result = new Select(result, predicate);
                iter.remove();
            }
        }

        return result;
    }

    private Set<Attribute> getProjectedAttributeSet(List<Predicate> predicates, Operator op) {
        Set<Attribute> result = new HashSet<>();
        for (Predicate predicate : predicates) {
            result.add(predicate.getLeftAttribute());
            if (!predicate.equalsValue()) {
                result.add(predicate.getRightAttribute());
            }
        }

        if (op instanceof Project) {
            result.addAll(((Project) op).getAttributes());
        }

        result.addAll(finalAttributes);
        return result;
    }

    private Operator moveDownProject(Operator op, Set<Attribute> projectedAttributeSet) {
        Operator result = op;
        List<Attribute> projectAttributes = new ArrayList<>(projectedAttributeSet);

        if (result.getOutput() == null ) result.accept(es);
        projectAttributes.retainAll(result.getOutput().getAttributes());

//        result = projectAttributes.size() > 0 && projectAttributes.size() < result.getOutput().getAttributes().size() ? new Project(result, projectAttributes) : result;

        // note that for q5, it will be select all
        // so if it is scan, we will make a project anyway
        result = projectAttributes.size() > 0 && (projectAttributes.size() < result.getOutput().getAttributes().size() || op instanceof Scan) ? new Project(result, projectAttributes) : result;

        if (result.getOutput() == null ) result.accept(es);
        return result;
    }

    private Operator reorder(List<Operator> operators, Set<Predicate> predicates) {
        int size = operators.size();
        Operator result = null;
        // The plan and remaining operators
        Queue<Map.Entry<Operator, List<Operator>>> joinQueue = new LinkedList<>();
        // remaining predicates
        Queue<Set<Predicate>> predicateQueue = new LinkedList<>();
        joinQueue.add(new AbstractMap.SimpleEntry<>(null, operators));
        predicateQueue.add(predicates);
        while (joinQueue.peek() != null) {
            Map.Entry<Operator, List<Operator>> entry = joinQueue.poll();
            Set<Predicate> predicateSet = predicateQueue.poll();
            Operator leftOp = entry.getKey();
            List<Operator> remainOps = entry.getValue();
//            System.out.println("remainOps: " + remainOps.size());
            if (remainOps.size() == 0) {
//                System.out.println("result: " + leftOp);
//                System.out.println("value: " + leftOp.getOutput().getTupleCount());
                if (result == null || leftOp.getOutput().getTupleCount() < result.getOutput().getTupleCount()) {
                    result = leftOp;
                }
            } else {
                for (Operator rightOp : remainOps) {
                    if (rightOp.getOutput() == null ) rightOp.accept(es);

                    List<Operator> remainOpsCopy = new ArrayList<>(remainOps);
                    assert predicateSet != null;
                    Set<Predicate> predicateSetCopy = new HashSet<>(predicateSet); // TODO: 似乎不需要,一会检查下子方法是否已经复制
                    if (leftOp == null) {
                        remainOpsCopy.remove(rightOp);
                        Operator projectOp = moveDownProject(rightOp, getProjectedAttributeSet(new ArrayList<>(predicateSetCopy), rightOp));

//                        joinQueue.add(new AbstractMap.SimpleEntry<>(rightOp, remainOpsCopy));
                        joinQueue.add(new AbstractMap.SimpleEntry<>(projectOp, remainOpsCopy));
                        predicateQueue.add(predicateSetCopy);

//                        System.out.println("entry: " + joinQueue.peek().getKey() + " | " + joinQueue.peek().getValue());
                    } else {
//                        remainOpsCopy.remove(leftOp);
//                        System.out.println("remainOps:"+ remainOpsCopy);
//                        joinQueue.add(new AbstractMap.SimpleEntry<>(leftOp, remainOpsCopy));
//                        System.out.println("size:"+ joinQueue.size());


//                        Operator newOp = findJoinOrProductPredicate(leftOp, rightOp, predicates);

                        if (leftOp.getOutput() == null ) leftOp.accept(es);

                        Operator newOp = null;

                        List<Attribute> attributes1 = leftOp.getOutput().getAttributes();
                        List<Attribute> attributes2 = rightOp.getOutput().getAttributes();

                        for (Predicate predicate : predicateSet) {
                            if (attributes1.contains(predicate.getLeftAttribute()) && attributes2.contains(predicate.getRightAttribute())) {

                                Operator leftProjectOp = moveDownProject(leftOp, getProjectedAttributeSet(new ArrayList<>(predicateSetCopy), leftOp));
                                Operator rightProjectOp = moveDownProject(rightOp, getProjectedAttributeSet(new ArrayList<>(predicateSetCopy), rightOp));
                                newOp = new Join(leftProjectOp, rightProjectOp, predicate);

                            }
                            // TODO: check if this is correct
                            else if (attributes1.contains(predicate.getRightAttribute()) && attributes2.contains(predicate.getLeftAttribute())) {
//                                predicateSetCopy.remove(predicate);
//                                System.out.println("predicateSetCopy:"+ predicateSetCopy);
                                Operator leftProjectOp = moveDownProject(leftOp, getProjectedAttributeSet(new ArrayList<>(predicateSetCopy), leftOp));
                                Operator rightProjectOp = moveDownProject(rightOp, getProjectedAttributeSet(new ArrayList<>(predicateSetCopy), rightOp));
                                newOp = new Join(rightProjectOp, leftProjectOp, predicate);

                            }

                            predicateSetCopy.remove(predicate);
//                            System.out.println("predicateSetCopy:"+ predicateSetCopy);
                            if (newOp != null) {
//                System.out.println("newOp:"+ newOp);
                                if (newOp.getOutput() == null ) newOp.accept(es);
//                System.out.println("--------------result-------------------");
//                                newOp.accept(inspector);
//                System.out.println("--------------end result-------------------");
                                break;
                            }
                        }


                        if (newOp == null) {
                            newOp = new Product(leftOp, rightOp);
                            if (newOp.getOutput() == null ) newOp.accept(es);
                        }

//                        System.out.println("--------------result-------------------");
//                        newOp.accept(inspector);
//                        System.out.println("--------------end result-------------------");




                        remainOpsCopy.remove(rightOp);
                        joinQueue.add(new AbstractMap.SimpleEntry<>(newOp, remainOpsCopy));

                        predicateQueue.add(predicateSetCopy);


                    }

//                    System.out.println("joinQueue: " + joinQueue);
//                    System.out.println("joinQueue.key: " + newOp.toString());
//                    System.out.println("joinQueue.value: " + joinQueue.peek().getValue());


                }
            }

        }

        System.out.println("result: " + result);
        System.out.println("result.getOutput(): " + finalAttributes);
        assert result != null;
        if (result.getOutput().getAttributes().size() > finalAttributes.size() && finalAttributes.size() > 0) {
           result = new Project(result, new ArrayList<>(finalAttributes));
           result.accept(es);
        }


        return result;
    }

    private Operator findJoinOrProductPredicate(Operator op1, Operator op2, Set<Predicate> predicates) {
        Operator result = null;

//        System.out.println("predicates: " + predicates);

        if (op1.getOutput() == null ) op1.accept(es);
        if (op2.getOutput() == null ) op2.accept(es);
        List<Attribute> attributes1 = op1.getOutput().getAttributes();
        List<Attribute> attributes2 = op2.getOutput().getAttributes();
//        System.out.println("attributes1: " + attributes1);
//        System.out.println("attributes2: " + attributes2);
//        System.out.println("----------------op1-----------------");
//        op1.accept(inspector);
//        System.out.println("----------------op2-----------------");
        op2.accept(inspector);
        for (Predicate predicate : predicates) {
//            System.out.println("-----------------findJoinOrProductPredicate----------------");
//            System.out.println("predicate: " + predicate);

            if (attributes1.contains(predicate.getLeftAttribute()) && attributes2.contains(predicate.getRightAttribute())) {
                result = new Join(op1, op2, predicate);
            }
            // TODO: check if this is correct
            else if (attributes1.contains(predicate.getRightAttribute()) && attributes2.contains(predicate.getLeftAttribute())) {
                result = new Join(op2, op1, predicate);
            }

            if (result != null) {
//                System.out.println("result:"+ result);
                if (result.getOutput() == null ) result.accept(es);
//                System.out.println("--------------result-------------------");
                result.accept(inspector);
//                System.out.println("--------------end result-------------------");
                break;
            }
        }

        if (result == null) {
            result = new Product(op1, op2);
            if (result.getOutput() == null ) result.accept(es);
        }

//        System.out.println("--------------result-------------------");
//        result.accept(inspector);
//        System.out.println("--------------end result-------------------");
        return result;
    }

    private List<Attribute> PredicateToAttributes(Set<Predicate> predicates) {
        List<Attribute> result = new ArrayList<>();
        for (Predicate predicate : predicates) {
            result.add(predicate.getLeftAttribute());
            if (!predicate.equalsValue()) {
                result.add(predicate.getRightAttribute());
            }
        }
        return result;
    }

}
