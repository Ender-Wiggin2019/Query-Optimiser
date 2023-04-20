package sjdb;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Oushuo Huang
 * **/

public class Optimiser implements PlanVisitor {
    private final Estimator es = new Estimator();
//    private Inspector inspector = new Inspector(); // debug only
    private final Set<Attribute> attributeSet = new HashSet<>();
    private final Set<Predicate> predicateSet = new HashSet<>();
    private final Set<Scan> scanSet = new HashSet<>();
    private List<Attribute> finalAttributes = new ArrayList<>();

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

        List<Operator> list = moveDownSelect();

        Operator result = reorderAndMoveDownProject(list, predicateSetCopy);

        System.out.println("reorder result: " + result);
        result.accept(es);
//        result.accept(inspector);
        System.out.println("reorder render: " + result.getOutput().render());
        return result;
    }

    // TODO: separate to 2 methods
    private List<Operator> moveDownSelect() {
        List<Operator> result = new ArrayList<>();

        for (Scan scan : scanSet) {
            Operator moveDownSelectOp = moveDownSelect(scan, predicateSet);
            result.add(moveDownSelectOp);
        }

        return result;
    }

    /**
     * @param op the operator to be optimised
     * @return the optimised operator, with updated `predicateSet`
     */
    private Operator moveDownSelect(Operator op, Set<Predicate> predicateSet) {
        Operator result = op;
        List<Attribute> attributes = result.getOutput().getAttributes();
        Iterator<Predicate> iter = predicateSet.iterator();

        while (iter.hasNext()) {
            Predicate predicate = iter.next();
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

        // note that for q5, it will be select all
        // so if it is scan, we will make a project anyway
        result = projectAttributes.size() > 0 && (projectAttributes.size() < result.getOutput().getAttributes().size() || op instanceof Scan) ? new Project(result, projectAttributes) : result;

        if (result.getOutput() == null ) result.accept(es);
        return result;
    }

    private Operator reorderAndMoveDownProject(List<Operator> operators, Set<Predicate> predicates) {
        Operator result = null;

        /*
         * I use 2 queues as date structure with the principle of dynamic programming
         * It will start from the leaf node and move up to the root node, with updating cost by `Estimator`
         * */
        Queue<Map.Entry<Operator, List<Operator>>> joinQueue = new LinkedList<>(); // Queue for joined operator, and remaining operators
        Queue<Set<Predicate>> predicateQueue = new LinkedList<>(); // Queue for remaining predicates

        // init state
        joinQueue.add(new AbstractMap.SimpleEntry<>(null, operators));
        predicateQueue.add(predicates);

        // start bfs, update cost by `Estimator`
        while (joinQueue.peek() != null) {
            // get current state
            Map.Entry<Operator, List<Operator>> entry = joinQueue.poll();
            Set<Predicate> predicateSet = predicateQueue.poll();
            Operator leftOp = entry.getKey();
            List<Operator> remainOps = entry.getValue();

            // find out the best join order by comparing cost from `getTupleCount()`
            if (remainOps.size() == 0) {
                if (result == null || leftOp.getOutput().getTupleCount() < result.getOutput().getTupleCount()) {
                    result = leftOp; // update result if it is better
                }
            } else {
                for (Operator rightOp : remainOps) {
                    if (rightOp.getOutput() == null ) rightOp.accept(es);

                    List<Operator> remainOpsCopy = new ArrayList<>(remainOps);

                    assert predicateSet != null;
                    Set<Predicate> predicateSetCopy = new HashSet<>(predicateSet);

                    // if leftOp is null, add the first operator
                    if (leftOp == null) {
                        remainOpsCopy.remove(rightOp);
                        Operator projectOp = moveDownProject(rightOp, getProjectedAttributeSet(new ArrayList<>(predicateSetCopy), rightOp));

                        //
                        joinQueue.add(new AbstractMap.SimpleEntry<>(projectOp, remainOpsCopy));
                        predicateQueue.add(predicateSetCopy);
                    } else {
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
                                Operator leftProjectOp = moveDownProject(leftOp, getProjectedAttributeSet(new ArrayList<>(predicateSetCopy), leftOp));
                                Operator rightProjectOp = moveDownProject(rightOp, getProjectedAttributeSet(new ArrayList<>(predicateSetCopy), rightOp));
                                newOp = new Join(rightProjectOp, leftProjectOp, predicate);

                            }

                            predicateSetCopy.remove(predicate);
                            if (newOp != null) {
                                if (newOp.getOutput() == null ) newOp.accept(es);
                                break;
                            }
                        }

                        if (newOp == null) {
                            newOp = new Product(leftOp, rightOp);
                            if (newOp.getOutput() == null ) newOp.accept(es);
                        }

                        remainOpsCopy.remove(rightOp);
                        joinQueue.add(new AbstractMap.SimpleEntry<>(newOp, remainOpsCopy));

                        predicateQueue.add(predicateSetCopy);
                    }
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
}
